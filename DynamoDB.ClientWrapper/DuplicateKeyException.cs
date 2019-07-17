namespace DynamoDB.ClientWrapper
{
    using System;
    
    public class DuplicateKeyException : Exception
    {
        public DuplicateKeyException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}