namespace DynamoDB.ClientWrapper
{
    using System;
    
    public class PrimaryKeyNameFailException : Exception
    {
        public PrimaryKeyNameFailException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}