namespace DynamoDB.ClientWrapper
{
    using System;
    
    public class SaveDataException : Exception
    {
        public SaveDataException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}