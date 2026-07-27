using System.Reflection;

namespace Wallaby.Internal;

/// <summary>
/// The running build's version, e.g. <c>1.0.0-rc.1+cf62c1c</c>. The commit hash the SDK appends to the
/// informational version is shortened, keeping the build traceable without a 40-character log line.
/// </summary>
internal static class WallabyVersion
{
    private const int ShortHashLength = 7;

    /// <summary>The informational version, or <c>unknown</c> when the attribute is absent.</summary>
    public static string Current { get; } = Resolve();

    private static string Resolve()
    {
        var informational = typeof(WallabyVersion).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrEmpty(informational))
        {
            return "unknown";
        }

        var plus = informational.IndexOf('+');
        if (plus < 0 || informational.Length - plus - 1 <= ShortHashLength)
        {
            return informational;
        }
        return informational[..(plus + 1 + ShortHashLength)];
    }
}
