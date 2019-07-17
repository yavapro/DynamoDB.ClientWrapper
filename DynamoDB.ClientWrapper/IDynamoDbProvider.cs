namespace DynamoDB.ClientWrapper
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    
    public interface IDynamoDbProvider
    {
        Task PutItemAsync<T>(string tableName, T item, IEnumerable<string> checkUniqueKeys);
    }
}