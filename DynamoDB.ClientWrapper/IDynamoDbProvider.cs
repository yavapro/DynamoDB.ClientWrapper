namespace DynamoDB.ClientWrapper
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    
    public interface IDynamoDbProvider
    {
        Task PutItemAsync<T>(string tableName, string keyName, T item, bool checkUniqueKey = false);

        Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject, TKey>(string tableName, string keyName, IEnumerable<TKey> keyValues);
    }
}