using System;
using System.Linq.Expressions;
using System.Text;

namespace SqliteORM.Specification
{
    public class Filter<T> : IFilter<T> where T : class, new()
    {

        public Filter()
        {
            _Query = new StringBuilder();
            EntityName = typeof(T).Name;
        }

        public void Add(Expression<Func<T, object>> memberExpression, object memberValue)
        {

            if (_Query.ToString() != string.Empty)
                _Query.Append(" AND ");

            _Query.Append(string.Format(" [{0}] = {1}", NameOf(memberExpression), memberValue == null ? "NULL" : string.Format("'{0}'", memberValue)));
        }

        public string EntityName { get; private set; }

        private readonly StringBuilder _Query;

        /// <summary>
        /// Returns SELECT statement with WHERE clause based on the expression passed; This is CommandText
        /// </summary>
        public string Query
        {
            get
            {
                return string.Format("SELECT * FROM [{0}] {1} {2};"
                    , EntityName
                    , _Query.ToString() == string.Empty ? string.Empty : "WHERE"
                    , _Query.ToString());
            }
        }

        private string NameOf(Expression<Func<T, object>> exp)
        {
            MemberExpression body = exp.Body as MemberExpression;

            if (body == null)
            {
                UnaryExpression ubody = (UnaryExpression)exp.Body;
                body = ubody.Operand as MemberExpression;
            }

            return body.Member.Name;
        }
    }
}
