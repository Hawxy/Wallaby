using System;
using System.Linq;
using Nuke.Common;
using Nuke.Common.CI;
using Nuke.Common.CI.GitHubActions;
using Nuke.Common.CI.GitHubActions.Configuration;
using Nuke.Common.IO;
using Nuke.Common.ProjectModel;
using Nuke.Common.Tooling;
using Nuke.Common.Tools.DotNet;
using Serilog;
using static Nuke.Common.Tools.DotNet.DotNetTasks;

[GitHubActions(
    "Build & Test",
    GitHubActionsImage.UbuntuLatest,
    OnPushBranches = ["main"],
    OnPullRequestBranches = ["main"],
    InvokedTargets = [nameof(Test), nameof(AotSmoke)])]
[GitHubActions(
    "Manual Nuget Push",
    GitHubActionsImage.UbuntuLatest,
    On = [GitHubActionsTrigger.WorkflowDispatch],
    InvokedTargets = [nameof(NugetPush)],
    ImportSecrets = [nameof(NugetApiKey)])]
class Build : NukeBuild
{
    public static int Main () => Execute<Build>(x => x.Compile);
    
    
    [Solution] readonly Solution Solution;
   
    AbsolutePath ArtifactsDirectory => RootDirectory / "artifacts";

    Target Clean => _ => _
        .Before(Restore)
        .Executes(() =>
        {
            ArtifactsDirectory.CreateOrCleanDirectory();
        });

    Target Restore => _ => _
        .Executes(() =>
        {
            DotNetRestore(s => s
                .SetProjectFile(Solution));
        });

    Target Compile => _ => _
        .DependsOn(Restore)
        .Executes(() =>
        {
            DotNetBuild(s => s
                .SetProjectFile(Solution)
                .SetConfiguration("Release")
                .EnableNoRestore());
        });
    
    
    Target Test => _ => _
        .DependsOn(Compile)
        .Executes(() =>
        {
            DotNetTest(s =>
            {
                var config = s
                    .AddProcessAdditionalArguments("--project", Solution);
    
                return config;

            });
        });
    
    Target AotSmoke => _ => _
        .Executes(() =>
        {
            var project = Solution.AllProjects.Single(x => x.Name == "Wallaby.AotSmokeTest");
            var output = ArtifactsDirectory / "aot-smoke";
            DotNetPublish(_ => _
                .SetProject(project)
                .SetConfiguration("Release")
                .SetOutput(output));

            var exe = output / (OperatingSystem.IsWindows() ? "Wallaby.AotSmokeTest.exe" : "Wallaby.AotSmokeTest");
            ProcessTasks.StartProcess(exe, workingDirectory: output).AssertZeroExitCode();
        });

    static readonly string[] PackableProjects =
    [
        "Wallaby",
        "Wallaby.Providers.EntityFrameworkCore",
        "Wallaby.Providers.Marten",
        "Wallaby.Sinks.Http",
        "Wallaby.Sinks.Kafka",
        "Wallaby.Sinks.Meilisearch",
        "Wallaby.AspNetCore.HealthChecks",
        "Wallaby.Testing",
    ];

    Target NugetPack => _ => _
        .DependsOn(Compile)
        .Executes(() =>
        {
            foreach (var name in PackableProjects)
            {
                var project = Solution.AllProjects.Single(x => x.Name == name);
                DotNetPack(_ => _
                    .SetProject(project)
                    .SetConfiguration("Release")
                    .EnableContinuousIntegrationBuild()
                    .SetOutputDirectory(ArtifactsDirectory));
            }
        });
    
    [Parameter("Nuget Api Key")] [Secret] readonly string NugetApiKey;

    Target NugetPush => _ => _
        .DependsOn(NugetPack)
        .Requires(() => !string.IsNullOrEmpty(NugetApiKey))
        .Executes(() =>
        {
            DotNetNuGetPush(_ => _
                .SetSource("https://api.nuget.org/v3/index.json")
                .SetTargetPath(ArtifactsDirectory / "*.nupkg")
                .EnableSkipDuplicate()
                .EnableNoSymbols()
                .SetApiKey(NugetApiKey));
        });

}
