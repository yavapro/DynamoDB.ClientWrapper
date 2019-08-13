namespace DynamoDB.ClientWrapper
{
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

        public async Task PutItemAsync(string tableName, object item, bool checkUniqueKey = false)
        {
            var jsonData = JsonConvert.SerializeObject(item);
            var document = Document.FromJson(jsonData);
            Table table;

            try
            {
                table = Table.LoadTable(dynamoDBClient, tableName);
            }
            catch (ResourceNotFoundException e)
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", e);
            }

            foreach (var k in table.Keys)
            {
                if (!document.ContainsKey(k.Key))
                {
                    throw new PrimaryKeyNameFailException($"Not found the primary key '{k.Key}' in saving data.", null);
                }
            }

            var keys = table.Keys.Select(k => k.Key).ToArray();

            var config = new PutItemOperationConfig();

            if (checkUniqueKey)
            {
                config.ConditionalExpression = new Expression
                {
                    ExpressionStatement = string.Join(" && ", keys.Select(k => $"attribute_not_exists({k})"))
                };
            }

            try
            {
                await table.PutItemAsync(document, config);
            }
            catch (ConditionalCheckFailedException e)
            {
                throw new DuplicateKeyException(
                    $"The source '{tableName}' has already contained data with keys '{string.Join(", ", keys)}'.",
                    e);
            }
        }

        public async Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject>(string tableName, IEnumerable<object> keyValues)
        {
            var keyValuesJsonData = keyValues.Select(JsonConvert.SerializeObject);
            var keyValuesDocument = keyValuesJsonData.Select(Document.FromJson);
            Table table;

            try
            {
                table = Table.LoadTable(dynamoDBClient, tableName);
            }
            catch (ResourceNotFoundException e)
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", e);
            }

            foreach (var k in table.Keys)
            {
                foreach (var document in keyValuesDocument)
                {
                    if (!document.ContainsKey(k.Key))
                    {
                        throw new PrimaryKeyNameFailException($"Not found the primary key '{k.Key}' in query.", null);
                    }
                }
            }

            var request = new BatchGetItemRequest();
            request.RequestItems = new Dictionary<string, KeysAndAttributes>
            {
                {
                    tableName,
                    new KeysAndAttributes
                    {
                        Keys = keyValuesDocument.Select(d => d.ToAttributeMap()).ToList()
                    }
                }
            };

            BatchGetItemResponse response = await dynamoDBClient.BatchGetItemAsync(request);

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
                catch (JsonReaderException e)
                {
                    throw new IncorrectDataFormatException(
                        $"'{typeof(TObject).Name}' and object '{jsonData}' have a different data format",
                        e);
                }

                resultItems.Add(data);
            }

            return resultItems;
        }

        public async Task UpdateItemAsync(string tableName, object item)
        {
            var jsonData = JsonConvert.SerializeObject(item);
            var document = Document.FromJson(jsonData);
            Table table;

            try
            {
                table = Table.LoadTable(dynamoDBClient, tableName);
            }
            catch (ResourceNotFoundException e)
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", e);
            }

            foreach (var k in table.Keys)
            {
                if (!document.ContainsKey(k.Key))
                {
                    throw new PrimaryKeyNameFailException($"Not found the primary key '{k.Key}' in saving data.", null);
                }
            }

            var keys = table.Keys.Select(k => k.Key).ToArray();

            var config = new UpdateItemOperationConfig
            {
                ConditionalExpression = new Expression
                {
                    ExpressionStatement = string.Join(" && ", keys.Select(k => $"attribute_exists({k})"))
                },
                ReturnValues = ReturnValues.None
            };

            try
            {
                await table.UpdateItemAsync(document, config);
            }
            catch (ConditionalCheckFailedException e)
            {
                throw new NotExistKeyException(
                    $"The source '{tableName}' has not contained data with keys '{string.Join(", ", keys)}'.",
                    e);
            }
        }
    }
}