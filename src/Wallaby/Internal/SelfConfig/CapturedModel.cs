using Microsoft.EntityFrameworkCore.Metadata;
using Wallaby.Model;

namespace Wallaby.Internal.SelfConfig;

/// <summary>
/// The capture model resolved once at startup: the raw EF Core <see cref="IModel"/> plus the derived
/// <see cref="CdcModel"/>. Registered as a singleton so the runtime and the backfill manager share one
/// instance.
/// </summary>
internal sealed record CapturedModel(IModel EfModel, CdcModel Cdc);
