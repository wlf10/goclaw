package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"log/slog"
	"maps"
	"os/exec"
	"strings"
	"sync"
	"time"
)

func CheckDockerAvailable(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker not available: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

type DockerSandbox struct {
	containerID string
	config      Config
	workspace   string
	createdAt   time.Time
	lastUsed    time.Time
	mu          sync.Mutex
}

func newDockerSandbox(ctx context.Context, name string, cfg Config, workspace string) (*DockerSandbox, error) {
	args := []string{"run", "-d", "--name", name, "--label", "goclaw.sandbox=true"}
	if cfg.ReadOnlyRoot {
		args = append(args, "--read-only")
	}
	for _, t := range cfg.Tmpfs {
		if !strings.Contains(t, ":") {
			opts := "noexec,nosuid,nodev"
			if cfg.TmpfsSizeMB > 0 {
				opts = fmt.Sprintf("size=%dm,%s", cfg.TmpfsSizeMB, opts)
			}
			t = fmt.Sprintf("%s:%s", t, opts)
		} else if !strings.Contains(t, "noexec") {
			t += ",noexec,nosuid,nodev"
		}
		args = append(args, "--tmpfs", t)
	}
	for _, cap := range cfg.CapDrop {
		args = append(args, "--cap-drop", cap)
	}
	args = append(args, "--security-opt", "no-new-privileges")
	if cfg.User != "" {
		args = append(args, "--user", cfg.User)
	}
	if cfg.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", cfg.MemoryMB))
	}
	if cfg.CPUs > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.1f", cfg.CPUs))
	}
	if cfg.PidsLimit > 0 {
		args = append(args, "--pids-limit", fmt.Sprintf("%d", cfg.PidsLimit))
	}
	if !cfg.NetworkEnabled {
		args = append(args, "--network", "none")
	}

	containerWorkdir := cfg.ContainerWorkdir()
	if workspace != "" && cfg.WorkspaceAccess != AccessNone {
		mountOpt := "rw"
		if cfg.WorkspaceAccess == AccessRO {
			mountOpt = "ro"
		}
		hostPath := resolveHostWorkspacePath(ctx, workspace)
		args = append(args, "-v", fmt.Sprintf("%s:%s:%s", hostPath, containerWorkdir, mountOpt))
	}
	args = append(args, "-w", containerWorkdir)

	// Bind-mount managed skills-store into sandbox.
	// When GoClaw runs on the host, resolveHostWorkspacePath returns the path as-is.
	if cfg.SkillsStoreDir != "" {
		hostSkillsPath := resolveHostWorkspacePath(ctx, cfg.SkillsStoreDir)
		skillsContainerPath := filepath.Join(containerWorkdir, ".managed-skills")
		args = append(args, "-v",
			fmt.Sprintf("%s:%s:ro", hostSkillsPath, skillsContainerPath))
	}

	for k, v := range cfg.Env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, cfg.Image, "sleep", "infinity")
	slog.Debug("creating sandbox container", "name", name, "args", args)
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("docker run failed: %w\nstderr: %s", err, stderr.String())
	}
	containerID := strings.TrimSpace(stdout.String())
	if len(containerID) > 12 {
		containerID = containerID[:12]
	}
	slog.Info("sandbox container created", "id", containerID, "name", name, "image", cfg.Image)
	if cfg.SetupCommand != "" {
		setupCmd := exec.CommandContext(ctx, "docker", "exec", "-i", containerID, "sh", "-lc", cfg.SetupCommand)
		if out, err := setupCmd.CombinedOutput(); err != nil {
			slog.Warn("sandbox setup command failed", "id", containerID, "error", err, "output", string(out))
		} else {
			slog.Info("sandbox setup command completed", "id", containerID)
		}
	}
	now := time.Now()
	return &DockerSandbox{containerID: containerID, config: cfg, workspace: workspace, createdAt: now, lastUsed: now}, nil
}

func (s *DockerSandbox) Exec(ctx context.Context, command []string, workDir string, opts ...ExecOption) (*ExecResult, error) {
	s.mu.Lock()
	s.lastUsed = time.Now()
	s.mu.Unlock()
	timeout := time.Duration(s.config.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	o := ApplyExecOpts(opts)
	args := []string{"exec"}
	for k, v := range o.Env {
		args = append(args, "-e", k+"="+v)
	}
	if workDir != "" {
		args = append(args, "-w", workDir)
	}
	args = append(args, s.containerID)
	args = append(args, command...)
	cmd := exec.CommandContext(execCtx, "docker", args...)
	maxOut := s.config.MaxOutputBytes
	if maxOut <= 0 {
		maxOut = 1 << 20
	}
	stdout := &limitedBuffer{max: maxOut}
	stderr := &limitedBuffer{max: maxOut}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("docker exec: %w", err)
		}
	}
	return &ExecResult{ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

func (s *DockerSandbox) Destroy(ctx context.Context) error {
	return exec.CommandContext(ctx, "docker", "rm", "-f", s.containerID).Run()
}

func (s *DockerSandbox) ID() string { return s.containerID }

type DockerManager struct {
	config    Config
	sandboxes map[string]*DockerSandbox
	mu        sync.RWMutex
	stopCh    chan struct{}
}

func NewDockerManager(cfg Config) *DockerManager {
	m := &DockerManager{config: cfg, sandboxes: make(map[string]*DockerSandbox), stopCh: make(chan struct{})}
	m.startPruning()
	return m
}

func (m *DockerManager) Get(ctx context.Context, key string, workspace string, cfgOverride *Config) (Sandbox, error) {
	cfg := m.config
	if cfgOverride != nil {
		cfg = *cfgOverride
	}
	if cfg.Mode == ModeOff {
		return nil, ErrSandboxDisabled
	}
	m.mu.RLock()
	if sb, ok := m.sandboxes[key]; ok {
		m.mu.RUnlock()
		return sb, nil
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb, ok := m.sandboxes[key]; ok {
		return sb, nil
	}
	prefix := cfg.ContainerPrefix
	if prefix == "" {
		prefix = "goclaw-sbx-"
	}
	sb, err := newDockerSandbox(ctx, prefix+sanitizeKey(key), cfg, workspace)
	if err != nil {
		return nil, err
	}
	m.sandboxes[key] = sb
	return sb, nil
}

func (m *DockerManager) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	sb, ok := m.sandboxes[key]
	if ok {
		delete(m.sandboxes, key)
	}
	m.mu.Unlock()
	if ok {
		return sb.Destroy(ctx)
	}
	return nil
}

func (m *DockerManager) ReleaseAll(ctx context.Context) error {
	m.mu.Lock()
	sbs := make(map[string]*DockerSandbox, len(m.sandboxes))
	maps.Copy(sbs, m.sandboxes)
	m.sandboxes = make(map[string]*DockerSandbox)
	m.mu.Unlock()
	for key, sb := range sbs {
		if err := sb.Destroy(ctx); err != nil {
			slog.Warn("failed to release sandbox", "key", key, "error", err)
		}
	}
	return nil
}

func (m *DockerManager) Stats() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	containers := make(map[string]string, len(m.sandboxes))
	for key, sb := range m.sandboxes {
		containers[key] = sb.containerID
	}
	return map[string]any{"mode": m.config.Mode, "image": m.config.Image, "active": len(m.sandboxes), "containers": containers}
}

func (m *DockerManager) Stop() {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
}

func (m *DockerManager) startPruning() {
	interval := time.Duration(m.config.PruneIntervalMin) * time.Minute
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				m.Prune(context.Background())
			}
		}
	}()
}

func (m *DockerManager) Prune(ctx context.Context) {
	// ... pruning logic unchanged
}

func sanitizeKey(key string) string {
	return strings.NewReplacer(":", "-", "/", "-", " ", "-", ".", "-").Replace(key)
}

type limitedBuffer struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (lb *limitedBuffer) Write(p []byte) (int, error) {
	if lb.truncated {
		return len(p), nil
	}
	remaining := lb.max - lb.buf.Len()
	if remaining <= 0 {
		lb.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		lb.buf.Write(p[:remaining])
		lb.truncated = true
		return len(p), nil
	}
	return lb.buf.Write(p)
}

func (lb *limitedBuffer) String() string {
	return lb.buf.String()
}
