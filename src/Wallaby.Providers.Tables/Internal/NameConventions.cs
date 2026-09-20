using System.Globalization;
using System.Text;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>Name derivation for members without an attribute or fluent override.</summary>
internal static class NameConventions
{
    /// <summary>
    /// The EFCore.NamingConventions rewriter: <c>OrderId</c> to <c>order_id</c>, <c>HTTPStatus</c> to
    /// <c>http_status</c>, <c>Line2Number</c> to <c>line2number</c>. An underscore precedes an upper-case letter
    /// that follows a lower-case letter, or that starts a new word after an acronym; a digit never introduces one.
    /// </summary>
    public static string ToSnakeCase(string name)
    {
        var builder = new StringBuilder(name.Length + Math.Min(2, name.Length / 5));
        var previousCategory = default(UnicodeCategory?);

        for (var currentIndex = 0; currentIndex < name.Length; currentIndex++)
        {
            var currentChar = name[currentIndex];
            if (currentChar == '_')
            {
                builder.Append('_');
                previousCategory = null;
                continue;
            }

            var currentCategory = char.GetUnicodeCategory(currentChar);
            switch (currentCategory)
            {
                case UnicodeCategory.UppercaseLetter:
                case UnicodeCategory.TitlecaseLetter:
                    if (previousCategory == UnicodeCategory.SpaceSeparator ||
                        previousCategory == UnicodeCategory.LowercaseLetter ||
                        previousCategory != UnicodeCategory.DecimalDigitNumber &&
                        previousCategory != null &&
                        currentIndex > 0 &&
                        currentIndex + 1 < name.Length &&
                        char.IsLower(name[currentIndex + 1]))
                    {
                        builder.Append('_');
                    }

                    currentChar = char.ToLower(currentChar, CultureInfo.InvariantCulture);
                    break;

                case UnicodeCategory.LowercaseLetter:
                case UnicodeCategory.DecimalDigitNumber:
                    if (previousCategory == UnicodeCategory.SpaceSeparator)
                    {
                        builder.Append('_');
                    }
                    break;

                default:
                    if (previousCategory != null)
                    {
                        previousCategory = UnicodeCategory.SpaceSeparator;
                    }
                    continue;
            }

            builder.Append(currentChar);
            previousCategory = currentCategory;
        }

        return builder.ToString();
    }
}
