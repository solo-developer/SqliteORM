using FastMember;
using RDPERP.POS.Desktop.Extensions.SqliteORM.Extensions;
using SqliteORM.CustomAttributes;
using SqliteORM.Dto;
using SqliteORM.Mapper;
using SqliteORM.Specification;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SqliteORM.Repository
{
    public static class Context
    {
        public static string ConnectionString { get => "data source=databaseFile.db"; }
    }

    public class BaseRepository<T> where T : class, new()
    {
        public SQLiteConnection GetConnection()
        {
            return new SQLiteConnection(Context.ConnectionString);
        }

        public string getTableName()
        {
            DbTableAttribute tableAttribute = getTableAttributes();
            if (tableAttribute == null)
            {
                Debug.WriteLine($"{typeof(T).Name} doesnot have a table attribute.");
                return typeof(T).Name;
            }
            if (string.IsNullOrWhiteSpace(tableAttribute.table_name))
            {
                Debug.WriteLine($"{typeof(T).Name} doesnot have a table_name attribute.");
                return typeof(T).Name;
            }
            if (tableAttribute.is_db_table == false)
            {
                Debug.WriteLine($"{typeof(T).Name} is not a database table.");
                return typeof(T).Name;
            }
            string tableName = tableAttribute.table_name;

            return tableName;
        }

        public long insert(T entity)
        {
            long identity = 0;
            bool hasIdentity = false;

            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();

            List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);

            foreach (PropertyInfo i in propertyInfos)
            {
                var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                if (ca != null)
                {
                    if (!ca.is_identity)
                    {
                        columns.Append(string.Format("[{0}],", i.Name));
                        values.Append(string.Format("{0},",
                               i.GetValue(entity) == null ? "NULL" : string.Format("'{0}'", i.GetValue(entity))));
                    }
                    else
                    {
                        hasIdentity = true;
                    }
                }
            }

            if (columns.ToString() != string.Empty)
            {

                columns.Remove(columns.Length - 1, 1); // Remove additional comma(',')
                values.Remove(values.Length - 1, 1); // Remove additional comma(',')

                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("INSERT INTO [{0}] ( {1} ) VALUES ( {2} ); SELECT last_insert_rowid();"
                    , getTableName(), columns, values));

                identity = hasIdentity ? execute(qry.ToString(), true) : execute(qry.ToString());
            }

            return identity;
        }

        public void deleteAll()
        {
            StringBuilder qry = new StringBuilder();
            qry.Append(string.Format("DELETE FROM [{0}]", getTableName()));
            execute(qry.ToString());
        }

        public void deleteRange(List<T> entities)
        {
            List<object> primaryKeyValues = getPrimaryKeyValues(entities);

            if (primaryKeyValues.Count == 0)
            {
                return;
            }

            string primaryKeyValuesAppendedByComma = string.Join(",", primaryKeyValues);

            string primaryKeyColumnName = getPrimaryKeyColumnName();

            string query = $"DELETE FROM {getTableName()} WHERE {primaryKeyColumnName} IN ({primaryKeyValuesAppendedByComma})";
            execute(query.ToString().Trim('"'));
        }

        public void insertRangeUsingSqlBulkCopy(List<T> entities)
        {
            SqliteExtensions extension = new SqliteExtensions(GetConnection());

            List<string> columns = new List<string>();

            T t = new T();
            List<PropertyInfo> propertyInfos = getPropertyInfoList(t);

            foreach (PropertyInfo i in propertyInfos)
            {
                var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                if (ca != null)
                {
                    columns.Add(i.Name);
                }
            }

            using (var reader = ObjectReader.Create(entities, columns.ToArray()))
            {
                extension.DestinationTableName = getTableName();
                extension.WriteToServer(reader);
            }

        }

        public void insertRange(List<T> entities)
        {
            StringBuilder qry = new StringBuilder();
            StringBuilder columns = new StringBuilder();
            StringBuilder query = new StringBuilder();
            string tableName = "";
            int j = 0;
            foreach (T entity in entities)
            {
                StringBuilder values = new StringBuilder();

                List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);
                tableName = getTableName();
                values.Clear();
                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                    if (ca != null)
                    {
                        if (!ca.is_identity)
                        {
                            if (j < propertyInfos.Count)
                            {
                                columns.Append(string.Format("[{0}],", i.Name));
                            }
                            values.Append(string.Format("'{0}',", i.GetValue(entity)));
                        }
                    }
                    j++;

                }
                values.Remove(values.Length - 1, 1); // Remove additional comma(',')
                query = query.Append(string.Format("({0}),"
                    , values));
            }

            if (columns.ToString() != string.Empty)
            {
                columns.Remove(columns.Length - 1, 1); // Remove additional comma(',')
                query.Remove(query.Length - 1, 1);

                qry.AppendLine(string.Format("INSERT INTO [{0}] ( {1} ) VALUES {2} ;"
                    , tableName, columns, query));
            }
            try
            {
                execute(qry.ToString().Trim('"'));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void insertRangeSingularly(List<T> entities)
        {
            foreach (T entity in entities)
            {
                StringBuilder columns = new StringBuilder();
                int j = 1;
                StringBuilder cmdQuery = new StringBuilder();

                StringBuilder values = new StringBuilder();

                List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);
                values.Clear();

                Dictionary<string, object> parameters = new Dictionary<string, object>();
                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                    if (ca != null)
                    {
                        if (!ca.is_identity)
                        {
                            if (j < propertyInfos.Count)
                            {
                                columns.Append(string.Format("{0},", i.Name));
                            }
                            else
                            {
                                columns.Append(string.Format("{0}", i.Name));
                            }
                            values.Append($"@{i.Name},");
                            parameters.Add(i.Name, i.GetValue(entity));
                        }
                    }
                    j++;
                }

                values.Remove(values.Length - 1, 1); // Remove additional comma(',')


                string CommandText = $"Insert into {getTableName()}({columns}) values ({values})";
                try
                {
                    executeSingleCommand(CommandText, parameters);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
        }

        public void insertRangeUsingCommand(List<T> entities)
        {
            string CommandText = string.Empty;
            int entityCount = 1;
            List<ParameterCarrier> parameters = new List<ParameterCarrier>();
            foreach (T entity in entities)
            {
                entityCount++;

                StringBuilder columns = new StringBuilder();
                int j = 1;
                StringBuilder cmdQuery = new StringBuilder();

                StringBuilder values = new StringBuilder();

                List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);
                values.Clear();

                int parametersCount = 0;


                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                    if (ca != null)
                    {
                        if (!ca.is_identity)
                        {
                            if (j < propertyInfos.Count)
                            {
                                columns.Append(string.Format("{0},", i.Name));
                            }
                            else
                            {
                                columns.Append(string.Format("{0}", i.Name));
                            }
                            values.Append($"@{i.Name}{entityCount},");

                            parameters.Add(new ParameterCarrier()
                            {
                                key = i.Name + entityCount,
                                value = i.GetValue(entity)
                            });
                            parametersCount++;
                        }
                    }
                    j++;
                }

                values.Remove(values.Length - 1, 1); // Remove additional comma(',')

                CommandText += $"Insert into {getTableName()}({columns}) values ({values});";

            }
            try
            {
                executeBulkCommand(CommandText, parameters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void update(T entity)
        {
            StringBuilder columns = new StringBuilder();
            StringBuilder clause = new StringBuilder();

            List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);
            foreach (PropertyInfo i in propertyInfos)
            {
                var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                if (ca != null)
                {
                    if (!ca.is_primary)
                    {
                        columns.Append(string.Format("[{0}] = {1},", i.Name,
                            i.GetValue(entity) == null ? "NULL" : string.Format("'{0}'", i.GetValue(entity))));
                    }
                    else
                    {
                        clause.Append(string.Format("[{0}] = '{1}'", i.Name, i.GetValue(entity)));
                    }
                }
            }

            if (columns.ToString() != string.Empty)
            {
                columns.Remove(columns.Length - 1, 1);
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("UPDATE [{0}] SET {1} WHERE {2};"
                    , getTableName(), columns, clause));


                execute(qry.ToString());
            }
        }

        public void updateRange(List<T> entities)
        {
            StringBuilder qry = new StringBuilder();
            foreach (T entity in entities)
            {
                StringBuilder columns = new StringBuilder();
                StringBuilder clause = new StringBuilder();


                #region MyRegion
                List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);
                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                    if (ca != null)
                    {
                        if (!ca.is_primary)
                        {
                            columns.Append(string.Format("[{0}] = {1},", i.Name,
                                i.GetValue(entity) == null ? "NULL" : string.Format("'{0}'", i.GetValue(entity))));
                        }
                        else
                        {
                            clause.Append(string.Format("[{0}] = '{1}'", i.Name, i.GetValue(entity)));
                        }
                    }
                }

                if (columns.ToString() != string.Empty)
                {
                    columns.Remove(columns.Length - 1, 1);

                    qry.AppendLine(string.Format("UPDATE [{0}] SET {1} WHERE {2};"
                        , getTableName(), columns, clause));
                }
                #endregion
            }

            execute(qry.ToString());
        }

        public void delete(object id)
        {
            T entity = new T();
            StringBuilder clause = new StringBuilder();

            List<PropertyInfo> pInfos = getPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                var pk = pi.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;
                if (pk != null && pk.is_primary)
                {
                    clause.Append(string.Format("[{0}]='{1}'", pi.Name, id));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("DELETE FROM [{0}] WHERE {1}", getTableName(), clause));
                execute(qry.ToString());
            }
        }


        public T getById(object id)
        {
            T entity = new T();
            StringBuilder clause = new StringBuilder();

            List<PropertyInfo> pInfos = getPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                var pk = pi.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;
                if (pk != null && pk.is_primary)
                {
                    clause.Append(string.Format("[{0}]='{1}'", pi.Name, id));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("SELECT * FROM [{0}] WHERE {1}", getTableName(), clause));
                var _entities = executeGet(qry.ToString());
                if (_entities != null && _entities.Count > 0)
                    entity = _entities[0];
            }


            return entity;
        }

        public List<T> find(IEnumerable<object> ids)
        {
            List<T> entities = new List<T>();
            StringBuilder clause = new StringBuilder();

            var entity = new T();
            List<PropertyInfo> pInfos = getPropertyInfoList(entity);

            foreach (var pi in pInfos)
            {
                var pk = pi.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;
                if (pk != null && pk.is_primary)
                {
                    string _ids = string.Empty;
                    foreach (var id in ids)
                    {
                        if (_ids != string.Empty)
                            _ids = _ids + ",";

                        _ids = _ids + id.ToString();
                    }

                    clause.Append(string.Format("[{0}] IN ({1})", pi.Name, _ids));
                    break;
                }
            }

            if (clause.ToString() != string.Empty)
            {
                StringBuilder qry = new StringBuilder();
                qry.Append(string.Format("SELECT * FROM [{0}] WHERE {1}", getTableName(), clause));
                entities = executeGet(qry.ToString());
            }

            return entities;
        }
        public List<T> find(IFilter<T> filter)
        {
            T entity = new T();
            return executeGet<T>(filter);
        }

        public List<T> getAll()
        {
            T entity = new T();
            return executeGet(string.Format("SELECT * FROM [{0}]", getTableName()));
        }

        public List<T> getAll(string commandText)
        {
            return executeGet(commandText);
        }
        public List<TEntity> getAll<TEntity>(string commandText)
            where TEntity : class, new()
        {
            return executeGet<TEntity>(commandText);
        }

        public DbTableAttribute getTableAttributes()
        {
            var dnAttribute = typeof(T).GetCustomAttribute(
                typeof(DbTableAttribute), true) as DbTableAttribute;

            return dnAttribute;
        }


        public void createTable()
        {
            DbTableAttribute tableAttribute = getTableAttributes();
            if (tableAttribute == null)
            {
                Debug.WriteLine($"{typeof(T).Name} doesnot have a table attribute.");
                return;
            }
            if (string.IsNullOrWhiteSpace(tableAttribute.table_name))
            {
                Debug.WriteLine($"{typeof(T).Name} doesnot have a table_name attribute.");
                return;
            }
            if (tableAttribute.is_db_table == false)
            {
                Debug.WriteLine($"{typeof(T).Name} is not a database table.");
                return;
            }
            string tableName = tableAttribute.table_name;

            string baseQuery = $@"CREATE TABLE IF NOT EXISTS [{tableName}](";
            string endQuery = ")";

            List<PropertyInfo> propertyInfos = typeof(T).GetProperties().ToList();

            foreach (PropertyInfo i in propertyInfos)
            {
                var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                if (ca != null && ca.is_db_column)
                {
                    baseQuery += $"{i.Name} ";

                    if (!string.IsNullOrWhiteSpace(ca.column_type))
                    {
                        baseQuery += $"{ca.column_type} ";
                    }

                    if (!ca.is_nullable)
                    {
                        baseQuery += "NOT ";
                    }
                    baseQuery += "NULL ";

                    if (ca.is_primary)
                    {
                        baseQuery += "PRIMARY KEY ";
                    }
                    if (ca.is_identity)
                    {
                        baseQuery += "AUTOINCREMENT";
                    }
                    baseQuery += ",";
                }
            }
            if (baseQuery.Contains(","))
            {
                int index = baseQuery.LastIndexOf(',');
                baseQuery = baseQuery.Remove(index, 1);
            }
            baseQuery += endQuery;
            execute(baseQuery);
        }

        public long execute(string cmdText, bool returnIdentity = false)
        {
            try
            {
                using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
                {
                    con.Open();
                    long id = 0;
                    using (SQLiteCommand cmd = new SQLiteCommand(cmdText.Trim('"'), con))
                    {
                        using (var transaction = con.BeginTransaction())
                        {
                            if (returnIdentity)
                            {
                                id = (long)cmd.ExecuteScalar();
                            }

                            else
                            {
                                cmd.ExecuteNonQuery();
                            }
                            transaction.Commit();
                        }
                    }
                    return id;
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public void executeSingleCommand(string cmd, Dictionary<string, object> parameters)
        {
            try
            {
                using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
                {
                    con.Open();

                    using (SQLiteCommand command = new SQLiteCommand(cmd, con))
                    {
                        foreach (var kvp in parameters)
                        {
                            string key = kvp.Key;
                            command.Parameters.Add(new SQLiteParameter(kvp.Key, kvp.Value));
                        }
                        using (var transaction = con.BeginTransaction())
                        {

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public void executeBulkCommand(string cmd, List<ParameterCarrier> parameters)
        {
            try
            {
                using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
                {
                    con.Open();

                    using (SQLiteCommand command = new SQLiteCommand(cmd, con))
                    {

                        foreach (var kvp in parameters)
                        {
                            string key = kvp.key;
                            command.Parameters.Add(new SQLiteParameter(kvp.key, kvp.value));
                        }
                        using (var transaction = con.BeginTransaction())
                        {

                            command.ExecuteNonQuery();

                            transaction.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public List<T> primaryKeyIn(List<object> values)
        {
            if (values.Count == 0)
            {
                return new List<T>();
            }

            string idsJoinedByComma = string.Join(",", values);

            using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
            {
                string primaryKeyColumnName = getPrimaryKeyColumnName();

                string cmdText = $"Select * from {getTableName()} where {primaryKeyColumnName} in {idsJoinedByComma}";

                con.Open();
                SQLiteCommand cmd = new SQLiteCommand(cmdText, con);
                using (var reader = cmd.ExecuteReader())
                {
                    return new EntityMapper().Map<T>(reader).ToList();
                }
            }
        }

        public List<T> columnValuesIn(Expression<Func<object>> expression, List<object> values)
        {
            PropertyInfo pi = ((MemberExpression)((UnaryExpression)expression.Body).Operand).Member as PropertyInfo;

            if (values.Count == 0)
            {
                return new List<T>();
            }

            string idsJoinedByComma = string.Join(",", values);

            using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
            {
                List<PropertyInfo> propertyInfos = typeof(T).GetProperties().ToList();

                var property = propertyInfos.Where(a => a.Name == pi.Name).SingleOrDefault();

                if (property == null)
                {
                    throw new Exception("Property doesnot exist.");
                }

                string columnName = property.Name;

                string cmdText = $"Select * from {getTableName()} where {columnName} in {idsJoinedByComma}";

                con.Open();
                SQLiteCommand cmd = new SQLiteCommand(cmdText, con);
                using (var reader = cmd.ExecuteReader())
                {
                    return new EntityMapper().Map<T>(reader).ToList();
                }
            }
        }

        public List<T> executeGet(string cmdText)
        {
            using (SQLiteConnection con = new SQLiteConnection(GetConnection()))
            {
                con.Open();
                SQLiteCommand cmd = new SQLiteCommand(cmdText, con);
                using (var reader = cmd.ExecuteReader())
                {
                    return new EntityMapper().Map<T>(reader).ToList();
                }
            }
        }

        public List<TEntity> executeGet<TEntity>(string cmdText)
            where TEntity : class, new()
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                SQLiteCommand cmd = new SQLiteCommand(cmdText, connection);
                using (var reader = cmd.ExecuteReader())
                {
                    return new EntityMapper().Map<TEntity>(reader).ToList();
                }
            }
        }

        public List<TEntity> executeGet<TEntity>(IFilter<TEntity> filter)
            where TEntity : class, new()
        {
            using (var connection = GetConnection())
            {
                connection.Open();
                SQLiteCommand cmd = new SQLiteCommand(filter.Query, connection);
                using (var reader = cmd.ExecuteReader())
                {
                    return new EntityMapper().Map<TEntity>(reader).ToList();
                }
            }
        }

        public List<TEntity> executeGet<TEntity>(SQLiteDataReader reader)
            where TEntity : class, new()
        {
            return new EntityMapper().Map<TEntity>(reader).ToList();
        }
        public List<PropertyInfo> getPropertyInfoList(T entity)
        {
            var adf = entity.GetType().GetProperties()
                 .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DbColumnAttribute)) != null).ToList();
            return adf;
        }
        public List<PropertyInfo> getPropertyInfoList<TEntity>(TEntity entity)
        {
            return entity.GetType().GetProperties()
                .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DbColumnAttribute)) != null).ToList();
        }

        public List<object> getPrimaryKeyValues(List<T> entities)
        {
            List<object> primaryKeyvalues = new List<object>();
            foreach (T entity in entities)
            {
                StringBuilder values = new StringBuilder();

                List<PropertyInfo> propertyInfos = getPropertyInfoList(entity);

                foreach (PropertyInfo i in propertyInfos)
                {
                    var ca = i.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute;

                    if (ca != null)
                    {
                        if (ca.is_primary)
                        {
                            primaryKeyvalues.Add(i.GetValue(entity));
                        }
                    }
                }
            }
            return primaryKeyvalues;
        }

        private string getPrimaryKeyColumnName()
        {
            List<PropertyInfo> propertyInfos = typeof(T).GetProperties().ToList();

            var primarykeyColumn = propertyInfos.Where(a => (a.GetCustomAttribute(typeof(DbColumnAttribute)) as DbColumnAttribute).is_primary).FirstOrDefault();

            if (primarykeyColumn == null)
            {
                throw new Exception("Table doesnot contain primary key column.");
            }

            string primaryKeyColumnName = primarykeyColumn.Name;
            return primaryKeyColumnName;
        }
    }
}
