namespace DynamoDB.ClientWrapper
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    
    public interface IDynamoDbProvider
    {
        Task PutItemAsync<T>(string tableName, T item);

        Task PutItemAsync<T>(string tableName, T item, string checkUniqueKey);
        
        Task PutItemAsync<T>(string tableName, T item, IEnumerable<string> checkUniqueKeys);

        Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject, TKey>(string tableName, string keyName, IEnumerable<TKey> keyValues);
    }
}