using System.Text;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>Name derivation for members without an attribute or fluent override.</summary>
internal static class NameConventions
{
    /// <summary>
    /// <c>OrderId</c> to <c>order_id</c>, <c>HTTPStatus</c> to <c>http_status</c>, <c>Line2Number</c> to
    /// <c>line2_number</c>: an underscore precedes an upper-case letter that follows a lower-case letter or
    /// digit, or that starts a new word after an acronym.
    /// </summary>
    public static string ToSnakeCase(string name)
    {
        var builder = new StringBuilder(name.Length + 4);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                {
                    var previous = name[i - 1];
                    var nextIsLower = i + 1 < name.Length && char.IsLower(name[i + 1]);
                    if (char.IsLower(previous) || char.IsDigit(previous) || (char.IsUpper(previous) && nextIsLower))
                    {
                        builder.Append('_');
                    }
                }
                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }
        return builder.ToString();
    }
}
