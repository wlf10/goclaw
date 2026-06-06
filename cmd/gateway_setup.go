package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/nextlevelbuilder/goclaw/internal/audio"
	"github.com/nextlevelbuilder/goclaw/internal/bootstrap"
	"github.com/nextlevelbuilder/goclaw/internal/bus"
	"github.com/nextlevelbuilder/goclaw/internal/config"
	mcpbridge "github.com/nextlevelbuilder/goclaw/internal/mcp"
	"github.com/nextlevelbuilder/goclaw/internal/permissions"
	"github.com/nextlevelbuilder/goclaw/internal/providers"
	"github.com/nextlevelbuilder/goclaw/internal/sandbox"
	"github.com/nextlevelbuilder/goclaw/internal/edition"
	"github.com/nextlevelbuilder/goclaw/internal/skills"
	"github.com/nextlevelbuilder/goclaw/internal/store"
	"github.com/nextlevelbuilder/goclaw/internal/store/pg"
	"github.com/nextlevelbuilder/goclaw/internal/tools"
	"github.com/nextlevelbuilder/goclaw/internal/tracing"
	"github.com/nextlevelbuilder/goclaw/internal/tts"
	"github.com/nextlevelbuilder/goclaw/pkg/browser"
	"github.com/nextlevelbuilder/goclaw/pkg/protocol"
)

func setupToolRegistry(
	cfg *config.Config,
	workspace string,
	providerRegistry *providers.Registry,
) (
	toolsReg *tools.Registry,
	execApprovalMgr *tools.ExecApprovalManager,
	mcpMgr *mcpbridge.Manager,
	sandboxMgr sandbox.Manager,
	browserMgr *browser.Manager,
	webFetchTool *tools.WebFetchTool,
	ttsTool *tools.TtsTool,
	audioMgr *audio.Manager,
	permPE *permissions.PolicyEngine,
	toolPE *tools.PolicyEngine,
	dataDir string,
	agentCfg config.AgentDefaults,
) {
	toolsReg = tools.NewRegistry()
	agentCfg = cfg.ResolveAgent("default")

	if sbCfg := cfg.Agents.Defaults.Sandbox; sbCfg != nil && sbCfg.Mode != "" && sbCfg.Mode != "off" {
		if err := sandbox.CheckDockerAvailable(context.Background()); err != nil {
			slog.Warn("sandbox disabled: Docker not available",
				"configured_mode", sbCfg.Mode,
				"error", err,
			)
		} else {
			resolved := sbCfg.ToSandboxConfig()
			sandboxMgr = sandbox.NewDockerManager(resolved)
			slog.Info("sandbox enabled", "mode", string(resolved.Mode), "image", resolved.Image, "scope", string(resolved.Scope))
		}
	}

	if sandboxMgr != nil {
		toolsReg.Register(tools.NewSandboxedReadFileTool(workspace, agentCfg.RestrictToWorkspace, sandboxMgr))
		toolsReg.Register(tools.NewSandboxedWriteFileTool(workspace, agentCfg.RestrictToWorkspace, sandboxMgr))
		toolsReg.Register(tools.NewSandboxedListFilesTool(workspace, agentCfg.RestrictToWorkspace, sandboxMgr))
		toolsReg.Register(tools.NewSandboxedEditTool(workspace, agentCfg.RestrictToWorkspace, sandboxMgr))
		toolsReg.Register(tools.NewSandboxedExecTool(workspace, agentCfg.RestrictToWorkspace, sandboxMgr))
	} else {
		toolsReg.Register(tools.NewReadFileTool(workspace, agentCfg.RestrictToWorkspace))
		toolsReg.Register(tools.NewWriteFileTool(workspace, agentCfg.RestrictToWorkspace))
		toolsReg.Register(tools.NewListFilesTool(workspace, agentCfg.RestrictToWorkspace))
		toolsReg.Register(tools.NewEditTool(workspace, agentCfg.RestrictToWorkspace))
		toolsReg.Register(tools.NewExecTool(workspace, agentCfg.RestrictToWorkspace))
	}

	toolsReg.Register(tools.NewMemorySearchTool())
	toolsReg.Register(tools.NewMemoryGetTool())
	toolsReg.Register(tools.NewMemoryExpandTool())
	toolsReg.Register(tools.NewKnowledgeGraphSearchTool())
	slog.Info("memory + knowledge graph tools registered (PG-backed)")

	if cfg.Tools.Browser.Enabled {
		var opts []browser.Option
		if cfg.Tools.Browser.RemoteURL != "" {
			opts = append(opts, browser.WithRemoteURL(cfg.Tools.Browser.RemoteURL))
			slog.Info("browser tool enabled", "remote", cfg.Tools.Browser.RemoteURL)
		} else {
			opts = append(opts, browser.WithHeadless(cfg.Tools.Browser.Headless))
			slog.Info("browser tool enabled", "headless", cfg.Tools.Browser.Headless)
		}
		if cfg.Tools.Browser.ActionTimeoutMs > 0 {
			opts = append(opts, browser.WithActionTimeout(time.Duration(cfg.Tools.Browser.ActionTimeoutMs)*time.Millisecond))
		}
		if cfg.Tools.Browser.IdleTimeoutMs > 0 {
			opts = append(opts, browser.WithIdleTimeout(time.Duration(cfg.Tools.Browser.IdleTimeoutMs)*time.Millisecond))
		} else if cfg.Tools.Browser.IdleTimeoutMs < 0 {
			opts = append(opts, browser.WithIdleTimeout(0))
		}
		if cfg.Tools.Browser.MaxPages > 0 {
			opts = append(opts, browser.WithMaxPages(cfg.Tools.Browser.MaxPages))
		}
		browserMgr = browser.New(opts...)
		toolsReg.Register(browser.NewBrowserTool(browserMgr))
	}

	webFetchTool = tools.NewWebFetchTool(tools.WebFetchConfig{
		Policy:         cfg.Tools.WebFetch.Policy,
		AllowedDomains: cfg.Tools.WebFetch.AllowedDomains,
		BlockedDomains: cfg.Tools.WebFetch.BlockedDomains,
	})
	toolsReg.Register(webFetchTool)
	slog.Info("web_fetch tool enabled", "policy", cfg.Tools.WebFetch.Policy, "blocked", len(cfg.Tools.WebFetch.BlockedDomains))

	toolsReg.Register(tools.NewReadImageTool(providerRegistry))
	toolsReg.Register(tools.NewCreateImageTool(providerRegistry))

	ttsMgr := setupTTS(cfg)
	if ttsMgr == nil {
		ttsMgr = tts.NewManager(tts.ManagerConfig{})
	}
	setupAudioExtras(cfg, ttsMgr)
	audio.BridgeLegacySTT(ttsMgr, cfg)
	audioMgr = ttsMgr

	toolsReg.Register(tools.NewCreateAudioTool(ttsMgr))

	ttsTool = tools.NewTtsTool(ttsMgr)
	toolsReg.Register(ttsTool)
	if ttsMgr.HasProviders() {
		slog.Info("tts enabled", "provider", ttsMgr.PrimaryProvider(), "auto", string(ttsMgr.AutoMode()))
	}

	if cfg.Tools.RateLimitPerHour > 0 {
		toolsReg.SetRateLimiter(tools.NewToolRateLimiter(cfg.Tools.RateLimitPerHour))
		slog.Info("tool rate limiting enabled", "per_hour", cfg.Tools.RateLimitPerHour)
	}

	if cfg.Tools.ScrubCredentials != nil && !*cfg.Tools.ScrubCredentials {
		toolsReg.SetScrubbing(false)
		slog.Info("credential scrubbing disabled")
	}

	if len(cfg.Tools.McpServers) > 0 {
		mcpMgr = mcpbridge.NewManager(toolsReg, mcpbridge.WithConfigs(cfg.Tools.McpServers))
		if err := mcpMgr.Start(context.Background()); err != nil {
			slog.Warn("mcp.startup_errors", "error", err)
		}
		slog.Info("MCP servers initialized", "configured", len(cfg.Tools.McpServers), "tools", len(mcpMgr.ToolNames()))
	}

	{
		approvalCfg := tools.DefaultExecApprovalConfig()
		if eaCfg := cfg.Tools.ExecApproval; eaCfg.Security != "" {
			approvalCfg.Security = tools.ExecSecurity(eaCfg.Security)
		}
		if eaCfg := cfg.Tools.ExecApproval; eaCfg.Ask != "" {
			approvalCfg.Ask = tools.ExecAskMode(eaCfg.Ask)
		}
		if len(cfg.Tools.ExecApproval.Allowlist) > 0 {
			approvalCfg.Allowlist = cfg.Tools.ExecApproval.Allowlist
		}
		execApprovalMgr = tools.NewExecApprovalManager(approvalCfg)

		if execTool, ok := toolsReg.Get("exec"); ok {
			if aa, ok := execTool.(tools.ApprovalAware); ok {
				aa.SetApprovalManager(execApprovalMgr, "default")
			}
		}
		slog.Info("exec approval enabled", "security", string(approvalCfg.Security), "ask", string(approvalCfg.Ask))
	}

	permPE = permissions.NewPolicyEngine(cfg.Gateway.OwnerIDs)
	toolPE = tools.NewPolicyEngine(&cfg.Tools)
	dataDir = cfg.ResolvedDataDir()
	os.MkdirAll(dataDir, 0755)

	if execTool, ok := toolsReg.Get("exec"); ok {
		if et, ok := execTool.(*tools.ExecTool); ok {
			et.SetGlobalShellDenyGroups(cfg.Tools.ShellDenyGroups)
			et.DenyPaths(dataDir, ".goclaw/")
			et.AllowPathExemptions(
				".goclaw/skills-store/",
				filepath.Join(dataDir, "skills-store")+"/",
				filepath.Join(dataDir, "tenants")+"/",
			)
			et.DenyPaths(
				filepath.Join(workspace, "memory.db"),
				filepath.Join(workspace, "memory.db-wal"),
				filepath.Join(workspace, "memory.db-shm"),
				filepath.Join(workspace, "config.json"),
				filepath.Join(workspace, "delegate"),
				filepath.Join(dataDir, "goclaw.db"),
				filepath.Join(dataDir, "goclaw.db-wal"),
				filepath.Join(dataDir, "goclaw.db-shm"),
			)
			if cfgPath := os.Getenv("GOCLAW_CONFIG"); cfgPath != "" {
				et.DenyPaths(cfgPath)
			}
		}
	}

	internalDenyPaths := []string{
		"config.json", "memory.db", "memory.db-wal", "memory.db-shm",
		"goclaw.db", "goclaw.db-wal", "goclaw.db-shm",
		"memory/", ".media/", ".uploads/", "delegate/",
	}
	readFileDenyPaths := []string{
		"config.json", "memory.db", "memory.db-wal", "memory.db-shm",
		"goclaw.db", "goclaw.db-wal", "goclaw.db-shm",
		"memory/", "delegate/",
	}
	if rf, ok := toolsReg.Get("read_file"); ok {
		if t, ok := rf.(*tools.ReadFileTool); ok {
			t.DenyPaths(readFileDenyPaths...)
		}
	}
	if wf, ok := toolsReg.Get("write_file"); ok {
		if t, ok := wf.(*tools.WriteFileTool); ok {
			t.DenyPaths(internalDenyPaths...)
		}
	}
	if lf, ok := toolsReg.Get("list_files"); ok {
		if t, ok := lf.(*tools.ListFilesTool); ok {
			t.DenyPaths(internalDenyPaths...)
		}
	}
	if ed, ok := toolsReg.Get("edit"); ok {
		if t, ok := ed.(*tools.EditTool); ok {
			t.DenyPaths(internalDenyPaths...)
		}
	}
	if sf, ok := toolsReg.Get("send_file"); ok {
		if t, ok := sf.(*tools.SendFileTool); ok {
			t.DenyPaths(internalDenyPaths...)
		}
	}

	return
}

func wireTracingAndCron(
	cfg *config.Config,
	stores *store.Stores,
	msgBus *bus.MessageBus,
	dataDir string,
) (*tracing.Collector, *tracing.SnapshotWorker) {
	var traceCollector *tracing.Collector
	if stores.Tracing != nil {
		traceCollector = tracing.NewCollector(stores.Tracing)
		traceCollector.OnFlush = func(traceIDs []uuid.UUID) {
			ids := make([]string, len(traceIDs))
			for i, id := range traceIDs {
				ids[i] = id.String()
			}
			msgBus.Broadcast(bus.Event{
				Name:    protocol.EventTraceUpdated,
				Payload: map[string]any{"trace_ids": ids},
			})
		}
		traceCollector.SetStatusBroadcaster(func(p tracing.TraceStatusPayload, tid uuid.UUID) {
			msgBus.Broadcast(bus.Event{
				Name:     protocol.EventTraceStatusChanged,
				Payload:  p,
				TenantID: tid,
			})
		})
		traceCollector.Start()
		slog.Info("LLM tracing enabled")
	}

	var snapshotWorker *tracing.SnapshotWorker
	if stores.Snapshots != nil {
		snapshotWorker = tracing.NewSnapshotWorker(stores.DB, stores.Snapshots)
		snapshotWorker.Start()

		go func() {
			count, err := snapshotWorker.Backfill(context.Background())
			if err != nil {
				slog.Warn("snapshot backfill failed", "error", err)
			} else if count > 0 {
				slog.Info("snapshot backfill complete", "hours", count)
			}
		}()
	}

	cronRetryCfg := cfg.Cron.ToRetryConfig()
	if stores.Cron != nil {
		stores.Cron.SetOnJob(nil)
		_ = cronRetryCfg
		if cfg.Cron.DefaultTimezone != "" {
			stores.Cron.SetDefaultTimezone(cfg.Cron.DefaultTimezone)
		}
	}

	if stores.ConfigSecrets != nil {
		if secrets, err := stores.ConfigSecrets.GetAll(context.Background()); err == nil && len(secrets) > 0 {
			cfg.ApplyDBSecrets(secrets)
			cfg.ApplyEnvOverrides()
			slog.Info("config secrets loaded from DB", "count", len(secrets))
		}
	}

	return traceCollector, snapshotWorker
}

func setupMemoryEmbeddings(
	pgStores *store.Stores,
	providerRegistry *providers.Registry,
) {
	if pgStores.Memory != nil {
		if embProvider := resolveEmbeddingProvider(pgStores.Providers, providerRegistry, pgStores.SystemConfigs); embProvider != nil {
			pgStores.Memory.SetEmbeddingProvider(embProvider)
			slog.Info("memory embeddings enabled", "provider", embProvider.Name(), "model", embProvider.Model())

			type backfiller interface {
				BackfillEmbeddings(ctx context.Context) (int, error)
			}
			if bf, ok := pgStores.Memory.(backfiller); ok {
				go func() {
					bgCtx := context.Background()
					count, err := bf.BackfillEmbeddings(bgCtx)
					if err != nil {
						slog.Warn("memory embeddings backfill failed", "error", err)
					} else if count > 0 {
						slog.Info("memory embeddings backfill complete", "chunks_updated", count)
					}
				}()
			}

			if pgTeamStore, ok := pgStores.Teams.(*pg.PGTeamStore); ok {
				pgTeamStore.SetEmbeddingProvider(embProvider)
				go func() {
					if count, err := pgTeamStore.BackfillTaskEmbeddings(context.Background()); err != nil {
						slog.Warn("task embeddings backfill failed", "error", err)
					} else if count > 0 {
						slog.Info("task embeddings backfill complete", "tasks_updated", count)
					}
				}()
			}

			if pgKG, ok := pgStores.KnowledgeGraph.(*pg.PGKnowledgeGraphStore); ok {
				pgKG.SetEmbeddingProvider(embProvider)
				go func() {
					if count, err := pgKG.BackfillKGEmbeddings(context.Background()); err != nil {
						slog.Warn("KG embeddings backfill failed", "error", err)
					} else if count > 0 {
						slog.Info("KG embeddings backfill complete", "entities_updated", count)
					}
				}()
			}

			if pgStores.Vault != nil {
				pgStores.Vault.SetEmbeddingProvider(embProvider)
				slog.Info("vault embeddings enabled", "provider", embProvider.Name())
			}

			if pgStores.Episodic != nil {
				pgStores.Episodic.SetEmbeddingProvider(embProvider)
				slog.Info("episodic embeddings enabled", "provider", embProvider.Name())
			}
		} else {
			slog.Warn("memory embeddings disabled (no API key), chunks stored without vectors")
		}
	}
}

func seedSystemConfigs(sc store.SystemConfigStore, ts store.TenantStore, cfg *config.Config) {
	syncSystemConfigs(sc, ts, cfg, true)
}

func loadBootstrapFiles(
	pgStores *store.Stores,
	workspace string,
	agentCfg config.AgentDefaults,
) []bootstrap.ContextFile {
	var contextFiles []bootstrap.ContextFile

	if pgStores.Agents != nil {
		bgCtx := context.Background()
		defaultAgent, agErr := pgStores.Agents.GetByKey(bgCtx, "default")
		if agErr == nil {
			dbFiles := bootstrap.LoadFromStore(bgCtx, pgStores.Agents, defaultAgent.ID)
			if len(dbFiles) > 0 {
				contextFiles = dbFiles
				slog.Info("bootstrap loaded from store", "count", len(dbFiles))
			} else {
				if _, seedErr := bootstrap.SeedToStore(bgCtx, pgStores.Agents, defaultAgent.ID, defaultAgent.AgentType); seedErr != nil {
					slog.Warn("failed to seed bootstrap to store", "error", seedErr)
				} else {
					contextFiles = bootstrap.LoadFromStore(bgCtx, pgStores.Agents, defaultAgent.ID)
					slog.Info("bootstrap seeded and loaded from store", "count", len(contextFiles))
				}
			}
		}
	}

	if len(contextFiles) == 0 {
		rawFiles := bootstrap.LoadWorkspaceFiles(workspace)
		truncCfg := bootstrap.TruncateConfig{
			MaxCharsPerFile: agentCfg.BootstrapMaxChars,
			TotalMaxChars:   agentCfg.BootstrapTotalMaxChars,
		}
		if truncCfg.MaxCharsPerFile <= 0 {
			truncCfg.MaxCharsPerFile = bootstrap.DefaultMaxCharsPerFile
		}
		if truncCfg.TotalMaxChars <= 0 {
			truncCfg.TotalMaxChars = bootstrap.DefaultTotalMaxChars
		}
		contextFiles = bootstrap.BuildContextFiles(rawFiles, truncCfg)
		slog.Info("bootstrap loaded from filesystem", "count", len(contextFiles))
	}

	{
		var loadedNames []string
		for _, cf := range contextFiles {
			loadedNames = append(loadedNames, fmt.Sprintf("%s(%d)", cf.Path, len(cf.Content)))
		}
		slog.Info("bootstrap context files", "count", len(contextFiles), "files", loadedNames)
	}

	return contextFiles
}

func setupSkillsSystem(
	cfg *config.Config,
	workspace string,
	dataDir string,
	pgStores *store.Stores,
	toolsReg *tools.Registry,
	providerRegistry *providers.Registry,
	msgBus *bus.MessageBus,
) (*skills.Loader, *tools.SkillSearchTool, string, string, string) {
	var bundledSkillsDir string

	globalSkillsDir := os.Getenv("GOCLAW_SKILLS_DIR")
	if globalSkillsDir == "" {
		globalSkillsDir = filepath.Join(dataDir, "skills")
	}
	builtinSkillsDir := os.Getenv("GOCLAW_BUILTIN_SKILLS_DIR")
	if builtinSkillsDir == "" {
		builtinSkillsDir = "/app/bundled-skills"
	}
	skillsLoader := skills.NewLoader(workspace, globalSkillsDir, builtinSkillsDir)
	skillSearchTool := tools.NewSkillSearchTool(skillsLoader)
	toolsReg.Register(skillSearchTool)
	toolsReg.Register(tools.NewUseSkillTool())
	slog.Info("skill_search tool registered", "skills", len(skillsLoader.ListSkills(context.Background())))

	if pgStores.Skills != nil {
		storeDirs := pgStores.Skills.Dirs()
		if len(storeDirs) > 0 {
			skillsLoader.SetManagedDir(storeDirs[0])
			skillsLoader.SetManagedSandboxPrefix("skills-store")
			slog.Info("skills-store directory wired into loader", "dir", storeDirs[0])

			bundledSkillsDir = os.Getenv("GOCLAW_BUNDLED_SKILLS_DIR")
			if bundledSkillsDir == "" {
				for _, candidate := range []string{"bundled-skills", "/app/bundled-skills", "skills"} {
					if info, err := os.Stat(candidate); err == nil && info.IsDir() {
						bundledSkillsDir = candidate
						break
					}
				}
			}
			if bundledSkillsDir != "" {
				if seederStore, ok := pgStores.Skills.(skills.SystemSkillStore); ok {
					seeder := skills.NewSeeder(bundledSkillsDir, storeDirs[0], seederStore)
					seeded, skipped, seededSkills, err := seeder.Seed(context.Background())
					if err != nil {
						slog.Warn("system skills seed failed", "error", err)
					} else {
						if seeded > 0 {
							slog.Info("system skills seeded", "seeded", seeded, "skipped", skipped)
						}
						if len(seededSkills) > 0 {
							seeder.CheckDepsAsync(seededSkills, msgBus)
						}
					}
				}
			}
		}
	}

	if pgStores.Skills != nil && edition.Current().TeamFullMode {
		if manageStore, ok := pgStores.Skills.(store.SkillManageStore); ok {
			storeDirs := pgStores.Skills.Dirs()
			if len(storeDirs) > 0 {
				toolsReg.Register(tools.NewPublishSkillTool(manageStore, storeDirs[0], dataDir, skillsLoader))
				slog.Info("publish_skill tool registered")
				toolsReg.Register(tools.NewSkillManageTool(manageStore, storeDirs[0], dataDir, skillsLoader))
				slog.Info("skill_manage tool registered")
			}
		}
	}

	if pgStores.Skills != nil {
		if sas, ok := pgStores.Skills.(store.SkillAccessStore); ok {
			skillSearchTool.SetSkillAccessStore(sas)
		}
		if pgSkills, ok := pgStores.Skills.(*pg.PGSkillStore); ok {
			if embProvider := resolveEmbeddingProvider(pgStores.Providers, providerRegistry, pgStores.SystemConfigs); embProvider != nil {
				pgSkills.SetEmbeddingProvider(embProvider)
				skillSearchTool.SetEmbeddingSearcher(pgSkills, embProvider)
				slog.Info("skill embeddings enabled", "provider", embProvider.Name())

				go func() {
					count, err := pgSkills.BackfillSkillEmbeddings(context.Background())
					if err != nil {
						slog.Warn("skill embeddings backfill failed", "error", err)
					} else if count > 0 {
						slog.Info("skill embeddings backfill complete", "skills_updated", count)
					}
				}()
			}
		}
	}

	return skillsLoader, skillSearchTool, globalSkillsDir, bundledSkillsDir, builtinSkillsDir
}
