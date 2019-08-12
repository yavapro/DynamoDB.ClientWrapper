using System.Net.Http;
using System.Reflection;

namespace DynamoDB.ClientWrapper
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    
    using System.Collections.Generic;
    using Amazon.DynamoDBv2.DocumentModel;
    using Newtonsoft.Json;
    
    public class DynamoDbProvider : IDynamoDbProvider
    {
        private readonly IAmazonDynamoDB dynamoDBClient;
        
        public DynamoDbProvider(IAmazonDynamoDB dynamoDBClient)
        {
            this.dynamoDBClient = dynamoDBClient;
        }

        public async Task PutItemAsync<T>(string tableName, string keyName, T item, bool checkUniqueKey = false)
        {
            var jsonData = JsonConvert.SerializeObject(item);
            var document = Document.FromJson(jsonData);

            var request = new PutItemRequest(tableName, document.ToAttributeMap());
            
            if (checkUniqueKey)
            {
                request.ConditionExpression = $"attribute_not_exists({keyName})";
            }

            try
            {
                await dynamoDBClient.PutItemAsync(request);
            }
            catch (ConditionalCheckFailedException e)
            {
                throw new DuplicateKeyException(
                    $"The source '{tableName}' has already contained data with key '{keyName}'.",
                    e);
            }
            catch (ResourceNotFoundException e)
            {
                throw new TableNameFailException(
                    $"Not found the source '{tableName}'.",
                    e);
            }
            catch (AmazonDynamoDBException e)
            {
                if (e.ErrorCode == "ValidationException")
                {
                    throw new PrimaryKeyNameFailException(
                        $"Not found the primary key in saving data.",
                        e);
                }
            }
        }

        public async Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject, TKey>(string tableName, string keyName, IEnumerable<TKey> keyValues)
        {
            var request = new BatchGetItemRequest();
            request.RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                {
                    tableName,
                    new KeysAndAttributes
                    {
                        Keys = keyValues.Select(
                            id =>
                                new Dictionary<string, AttributeValue>
                                {
                                    { keyName, new AttributeValue
                                    {
                                        N = IsNumeric(typeof(TKey)) ? id.ToString() : null,
                                        S = IsNumeric(typeof(TKey)) ? null : id.ToString()
                                    } }
                                }
                        ).ToList()
                    }
                }
            };

            BatchGetItemResponse response = null; 
            
            try
            {
                response = await dynamoDBClient.BatchGetItemAsync(request);
            }
            catch (ResourceNotFoundException e)
            {
                throw new TableNameFailException(
                    $"Not found the source '{tableName}'.",
                    e);
            }
            catch (AmazonDynamoDBException e)
            {
                if (e.ErrorCode == "ValidationException")
                {
                    throw new PrimaryKeyNameFailException(
                        $"Not found the primary key '{keyName}' in the source '{tableName}'.",
                        e);
                }
            }
            
            var items = response.Responses[tableName];
            var resultItems = new List<TObject>();
            
            foreach (var item in items)
            {
                var document = Document.FromAttributeMap(item);
                var jsonData = document.ToJson();
                TObject data;
                
                try
                {
                    data = JsonConvert.DeserializeObject<TObject>(jsonData);
                }
                catch(JsonReaderException e)
                {
                    throw new IncorrectDataFormatException(
                        $"'{typeof(TObject).Name}' and object '{jsonData}' have a different data format", 
                        e);
                }
                
                resultItems.Add(data);
            }

            return resultItems;
        }
        
        private static bool IsNumeric(Type type)
        {
            if (type == null)
            {
                return false;
            }

            var typeCode = Type.GetTypeCode(type);

            switch (typeCode)
            {
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
            }
            
            return false;
        }
    }
}