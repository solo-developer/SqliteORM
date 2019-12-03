using SqliteORM.CustomAttributes;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Reflection;

namespace SqliteORM.Mapper
{
    public class EntityMapper
    {

        public IList<T> Map<T>(SQLiteDataReader reader)
            where T : class, new()
        {
            IList<T> collection = new List<T>();
            while (reader.Read())
            {
                T obj = new T();
                foreach (PropertyInfo i in obj.GetType().GetProperties()
                    .Where(p => p.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DbColumnAttribute)) != null).ToList())
                {
                    try
                    {
                        var ca = i.GetCustomAttribute(typeof(DbColumnAttribute));

                        if (ca != null)
                        {
                            if (((DbColumnAttribute)ca).convert == true)
                            {
                                if (reader[i.Name] != DBNull.Value)
                                    i.SetValue(obj, Convert.ChangeType(reader[i.Name], i.PropertyType));
                            }
                            else
                            {
                                if (reader[i.Name] != DBNull.Value)
                                    i.SetValue(obj, reader[i.Name]);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
#if DEBUG
                        Console.WriteLine(ex.Message);
                        Console.ReadLine();
#endif

#if !DEBUG
                        throw ex;
#endif
                    }
                }

                collection.Add(obj);
            }

            return collection;
        }

    }
}
