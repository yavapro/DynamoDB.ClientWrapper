namespace DynamoDB.ClientWrapper
{
    using System;
    
    public class TableNameFailException : Exception
    {
        public TableNameFailException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}