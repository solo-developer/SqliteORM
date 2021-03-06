﻿using SqliteORM.Repository;
using System;
using System.Linq;
using System.Reflection;

namespace SqliteORM.Helper
{
    public class DatabaseHelper
    {
        public static void createTables(string nameSpace)
        {
            try
            {
                var allClasses = Assembly.GetExecutingAssembly().GetTypes().Where(a => a.IsClass && a.Namespace != null && a.Namespace.Contains(nameSpace)).ToList();

                foreach (var type in allClasses)
                {
                    Type unboundGenericType = typeof(BaseRepository<>);
                    Type boundGenericType = unboundGenericType.MakeGenericType(type);
                    MethodInfo doSomethingMethod = boundGenericType.GetMethod("createTable");
                    object instance = Activator.CreateInstance(boundGenericType);
                    doSomethingMethod.Invoke(instance, null);
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }
    }
}
