using System;
using System.Collections.Generic;
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
    "Postgres Matrix",
    GitHubActionsImage.UbuntuLatest,
    OnPushBranches = ["main"],
    OnCronSchedule = "0 3 * * 1",
    InvokedTargets = [nameof(TestPostgresMatrix)])]
[GitHubActions(
    "Release",
    GitHubActionsImage.UbuntuLatest,
    OnPushTags = ["v*"],
    InvokedTargets = [nameof(Test), nameof(AotSmoke), nameof(NugetPush)],
    ImportSecrets = [nameof(NugetApiKey)])]
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
    
    
    [Parameter("Postgres Docker image for the integration suites (sets WALLABY_TEST_PG_IMAGE; default postgres:17)")]
    readonly string PostgresImage;

    /// <summary>
    /// The oldest supported major and the newest, run beside the default 17 by the matrix workflow.
    /// Docker Hub has no postgres:19 tag until GA; switch to it then.
    /// </summary>
    static readonly string[] PostgresMatrixImages = ["postgres:15", "postgres:19beta3"];

    Target Test => _ => _
        .DependsOn(Compile)
        .Executes(() => RunTests(PostgresImage));

    Target TestPostgresMatrix => _ => _
        .DependsOn(Compile)
        .Executes(() =>
        {
            foreach (var image in PostgresMatrixImages)
            {
                Log.Information("Running the test suite against {Image}", image);
                RunTests(image);
            }
        });

    void RunTests(string postgresImage)
    {
        DotNetTest(s =>
        {
            // Release, matching Compile: reuses its output instead of a second Debug build, and
            // tests the configuration that ships.
            var config = s
                .AddProcessAdditionalArguments("--project", Solution)
                .AddProcessAdditionalArguments("--configuration", "Release");

            if (postgresImage is not null)
            {
                config = config.SetProcessEnvironmentVariable("WALLABY_TEST_PG_IMAGE", postgresImage);
            }

            if (IsServerBuild)
            {
                // CI runners have 2 vCPUs; running the Testcontainers-backed suites in parallel
                // oversubscribes them and starves timing-sensitive e2e tests.
                config = config.AddProcessAdditionalArguments("--max-parallel-test-modules", "1");
            }

            return config;
        });
    }
    
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

    // Every project under src/ ships as a package; tests/ and samples/ never do.
    IEnumerable<Project> PackableProjects => Solution.AllProjects
        .Where(x => x.Directory.Parent == RootDirectory / "src")
        .OrderBy(x => x.Name);

    Target NugetPack => _ => _
        .DependsOn(Compile)
        .Executes(() =>
        {
            foreach (var project in PackableProjects)
            {
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
            AssertReleaseTagMatchesPackageVersion();

            // PDBs are embedded in the assemblies (DotNet.ReproducibleBuilds), so there is no symbols
            // package to push.
            DotNetNuGetPush(_ => _
                .SetSource("https://api.nuget.org/v3/index.json")
                .SetTargetPath(ArtifactsDirectory / "*.nupkg")
                .EnableSkipDuplicate()
                .EnableNoSymbols()
                .SetApiKey(NugetApiKey));
        });

    // A release tag must name the version in Package.Build.props: with skip-duplicate on, a mismatch
    // would otherwise push nothing and still report success.
    void AssertReleaseTagMatchesPackageVersion()
    {
        var reference = GitHubActions.Instance?.Ref;
        if (reference is null || !reference.StartsWith("refs/tags/v"))
        {
            return;
        }

        var version = XmlTasks.XmlPeekSingle(RootDirectory / "Package.Build.props", "/Project/PropertyGroup/Version");
        Assert.True(reference == $"refs/tags/v{version}",
            $"Release tag '{reference}' does not match the package version {version} in Package.Build.props.");
    }

}
