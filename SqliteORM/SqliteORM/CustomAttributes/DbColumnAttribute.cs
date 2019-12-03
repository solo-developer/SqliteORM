using System;

namespace SqliteORM.CustomAttributes
{
    public class DbColumnAttribute : Attribute
    {
        /// <summary>
        /// Set true if implicit conversion is required.
        /// </summary>
        public bool convert { get; set; }

        public bool is_primary { get; set; } = false;

        public bool is_identity { get; set; }

        public string column_type { get; set; }

        public bool is_db_column { get; set; } = true;
        public bool is_nullable { get; set; } = true;
    }
}
