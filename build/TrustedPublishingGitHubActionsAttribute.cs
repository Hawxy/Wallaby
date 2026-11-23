using System.Collections.Generic;
using System.Linq;
using Fallout.Common.CI.GitHubActions;
using Fallout.Common.CI.GitHubActions.Configuration;
using Fallout.Common.Execution;

/// <summary>
/// Generates a GitHub Actions workflow that authenticates to nuget.org via trusted publishing.
/// The NuGet/login action exchanges the workflow's OIDC token for a short-lived API key,
/// which is passed to the build as the NugetApiKey parameter.
/// See https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing
/// </summary>
class TrustedPublishingGitHubActionsAttribute : GitHubActionsAttribute
{
    private const string LoginStepId = "nuget_login";

    public TrustedPublishingGitHubActionsAttribute(string name, GitHubActionsImage image, params GitHubActionsImage[] images)
        : base(name, image, images)
    {
        // id-token: write lets the job request an OIDC token; contents: read is still needed for checkout
        WritePermissions = [GitHubActionsPermissions.IdToken];
        ReadPermissions = [GitHubActionsPermissions.Contents];
    }

    /// <summary>nuget.org profile name that owns the trusted publishing policy, or a ${{ secrets.* }} expression.</summary>
    public string NugetUser { get; set; }

    protected override GitHubActionsJob GetJobs(GitHubActionsImage image, IReadOnlyCollection<ExecutableTarget> relevantTargets)
    {
        var job = base.GetJobs(image, relevantTargets);

        // The issued API key is valid for 1 hour, so log in right before the build runs
        var runStepIndex = job.Steps.ToList().FindIndex(x => x is GitHubActionsRunStep);
        var login = new GitHubActionsCustomStep
        {
            Name = "NuGet login (trusted publishing)",
            Id = LoginStepId,
            Uses = "NuGet/login@v1",
            With = { ["user"] = NugetUser },
        };
        job.Steps = job.Steps.Take(runStepIndex)
            .Append(login)
            .Concat(job.Steps.Skip(runStepIndex))
            .ToArray();

        return job;
    }

    protected override IEnumerable<(string Key, string Value)> GetImports()
    {
        foreach (var import in base.GetImports())
            yield return import;

        yield return ("NugetApiKey", $"${{{{ steps.{LoginStepId}.outputs.NUGET_API_KEY }}}}");
    }
}
