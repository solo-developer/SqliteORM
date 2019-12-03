using System;

namespace SqliteORM.CustomAttributes
{
    public class DbTableAttribute : Attribute
    {
        public string table_name { get; set; }
        public bool is_db_table { get; set; } = true;
       
    }
}
