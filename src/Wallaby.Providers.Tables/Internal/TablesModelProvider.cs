using System.Collections.Frozen;
using Wallaby.Model;

namespace Wallaby.Providers.Tables.Internal;

/// <summary>
/// The plain-table storage provider: answers from the registrations <c>UseTables</c> built, applies
/// column selections, and materializes rows with <see cref="TablesRowMaterializer"/>.
/// </summary>
internal sealed class TablesModelProvider(IReadOnlyDictionary<Type, TableRegistration> registrations) : IWallabyModelProvider
{
    public CapturePlan BuildCapturePlan(CaptureSpec spec)
    {
        if (spec.DeclaredDependencies.Count > 0)
        {
            throw new WallabyConfigurationException(
                $"DependsOn is not supported for plain tables ('{spec.DeclaredDependencies.Keys.First().Name}' " +
                "declares one); a POCO has no navigations to resolve.");
        }
        if (spec.RequiresTenantColumn.Count > 0)
        {
            throw new WallabyConfigurationException(
                $"ScopedByTenant() is not supported for plain tables ('{spec.RequiresTenantColumn.First().Name}' " +
                "declares it); scope by a property instead, e.g. ScopedBy(e => e.TenantId).");
        }

        var plans = spec.DeclaredEntities.Select(type => BuildTablePlan(Registration(type, "Mapped entity"), spec)).ToList();
        return new CapturePlan
        {
            Model = new WallabyModel([.. plans.Select(p => p.Table)]),
            Materializer = new TablesRowMaterializer(plans),
        };
    }

    public QualifiedTable ResolveTable(Type entityClrType) => Registration(entityClrType, "Entity").QualifiedTable;

    public IReadOnlyList<QualifiedTable> ResolveAllTables()
        => registrations.Values.Select(r => r.QualifiedTable).Distinct().ToList();

    public bool Handles(Type entityClrType) => registrations.ContainsKey(entityClrType);

    private TableRegistration Registration(Type type, string what)
        => registrations.GetValueOrDefault(type)
            ?? throw new WallabyConfigurationException(
                $"{what} '{type.FullName}' is not registered with UseTables(...); add t.Add<{type.Name}>().");

    private static TablePlan BuildTablePlan(TableRegistration registration, CaptureSpec spec)
    {
        var members = registration.Members;
        var captured = spec.DeclaredColumnSelections.TryGetValue(registration.ClrType, out var selections)
            ? members.Where(ConsumedMembers(registration, selections).Contains).ToList()
            : members.ToList();

        var columnsByName = new Dictionary<string, int>(captured.Count, StringComparer.Ordinal);
        var columns = new List<CapturedColumn>(captured.Count);
        for (var i = 0; i < captured.Count; i++)
        {
            columnsByName[captured[i].ColumnName] = i;
            columns.Add(new CapturedColumn
            {
                PropertyName = captured[i].PropertyName,
                ColumnName = captured[i].ColumnName,
                ClrType = captured[i].ClrType,
            });
        }

        var constructorMembers = registration.Constructor.ParameterMembers;
        var constructorArguments = new int[constructorMembers.Length];
        var viaConstructor = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < constructorMembers.Length; i++)
        {
            if (constructorMembers[i] < 0)
            {
                constructorArguments[i] = -1;
                continue;
            }
            var member = members[constructorMembers[i]];
            viaConstructor.Add(member.PropertyName);
            constructorArguments[i] = captured.IndexOf(member);
        }
        var assignments = new List<int>();
        for (var i = 0; i < captured.Count; i++)
        {
            if (captured[i].CanSet && !viaConstructor.Contains(captured[i].PropertyName))
            {
                assignments.Add(i);
            }
        }

        return new TablePlan
        {
            Table = new CapturedTable
            {
                EntityClrType = registration.ClrType,
                Schema = registration.Schema,
                TableName = registration.Table,
                Columns = columns,
                PrimaryKey = registration.Key.Select(k => columns[columnsByName[k.ColumnName]]).ToList(),
                ColumnsNarrowed = captured.Count < members.Count,
                RequiresFullReplicaIdentity = spec.RequiresFullReplicaIdentity.Contains(registration.ClrType),
                RequiresMaterializedEntity = spec.RequiresMaterializedEntity.Contains(registration.ClrType),
            },
            Registration = registration,
            Captured = captured,
            ColumnsByName = columnsByName.ToFrozenDictionary(StringComparer.Ordinal),
            ConstructorArguments = constructorArguments,
            Assignments = [.. assignments],
        };
    }

    // Key members are always captured. Include adds the named members; Exclude adds every member but the
    // named ones and rejects a key member. The union across the type's mappings is what streams.
    private static HashSet<MemberPlan> ConsumedMembers(TableRegistration registration, IReadOnlyList<ColumnSelection> selections)
    {
        var byName = registration.Members.ToDictionary(m => m.PropertyName, StringComparer.Ordinal);
        var consumed = new HashSet<MemberPlan>(registration.Key);
        foreach (var selection in selections)
        {
            var method = selection.Mode == ColumnSelectionMode.Include ? "Consumes" : "ConsumesAllExcept";
            var named = new HashSet<MemberPlan>();
            foreach (var name in selection.PropertyNames)
            {
                var member = byName.GetValueOrDefault(name)
                    ?? throw new WallabyConfigurationException(
                        $"{method}<{registration.ClrType.Name}>(...) names '{name}', which is not a mapped property.");
                if (selection.Mode == ColumnSelectionMode.Exclude && member.IsKey)
                {
                    throw new WallabyConfigurationException(
                        $"{method}<{registration.ClrType.Name}>(...) cannot exclude key property '{name}'.");
                }
                named.Add(member);
            }

            if (selection.Mode == ColumnSelectionMode.Include)
            {
                consumed.UnionWith(named);
            }
            else
            {
                consumed.UnionWith(registration.Members.Where(m => !named.Contains(m)));
            }
        }
        return consumed;
    }
}
