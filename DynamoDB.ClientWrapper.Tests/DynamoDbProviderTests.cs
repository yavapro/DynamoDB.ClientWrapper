namespace DynamoDB.ClientWrapper.Tests
{
    using Amazon.DynamoDBv2;
    using Microsoft.Extensions.Configuration;
    using FluentAssertions;
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Xunit;

    public class DynamoDbProviderTests
    {
        private readonly DynamoDbProvider target;
        private const string tableWithPrimaryKeyId = "Test";

        public DynamoDbProviderTests()
        {
            var awsOptions = Configuration.Current.GetAWSOptions();
            var dynamoDbClient = awsOptions.CreateServiceClient<IAmazonDynamoDB>();

            target = new DynamoDbProvider(dynamoDbClient);
        }

        [Fact]
        public async Task PutItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableWithPrimaryKeyId, data);
        }

        [Fact]
        public async Task PutItemAsync_FailDuplicate_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableWithPrimaryKeyId, data);

            await Assert.ThrowsAsync<DuplicateKeyException>(
                () =>
                    target.PutItemAsync(tableWithPrimaryKeyId, data, true));
        }

        [Fact]
        public async Task PutItemAsync_FailTableName_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await Assert.ThrowsAsync<TableNameFailException>(
                () =>
                    target.PutItemAsync(Guid.NewGuid().ToString(), data));
        }

        [Fact]
        public async Task PutItemAsync_FailKeyName_Test()
        {
            var data = new
            {
                Value = Guid.NewGuid().GetHashCode()
            };

            await Assert.ThrowsAsync<PrimaryKeyNameFailException>(
                () =>
                    target.PutItemAsync(tableWithPrimaryKeyId, data));
        }

        [Fact]
        public async Task GetBatchItemsAsync_CheckEmptyResult_Test()
        {
            var responce = await target.GetBatchItemsAsync<TestData>(tableWithPrimaryKeyId, new[] {Query.Empty});

            Assert.NotNull(responce);
            Assert.Empty(responce);
        }

        [Fact]
        public async Task GetBatchItemsAsync_CheckGetSameResult_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableWithPrimaryKeyId, data);

            var query = new Query
            {
                Id = data.Id
            };

            var responce = await target.GetBatchItemsAsync<TestData>(tableWithPrimaryKeyId, new[] {query});

            Assert.NotNull(responce);
            Assert.NotEmpty(responce);
            Assert.Single(responce);
            responce.First().Should().BeEquivalentTo(data);
        }

        [Fact]
        public async Task GetBatchItemsAsync_FailTableName_Test()
        {
            await Assert.ThrowsAsync<TableNameFailException>(
                () =>
                    target.GetBatchItemsAsync<TestData>(Guid.NewGuid().ToString(), new[] {Query.Empty}));
        }

        [Fact]
        public async Task GetBatchItemsAsync_FailKeyName_Test()
        {
            await Assert.ThrowsAsync<PrimaryKeyNameFailException>(
                () =>
                    target.GetBatchItemsAsync<TestData>(tableWithPrimaryKeyId, new[] {new {NotKey = 1}}));
        }

        [Fact]
        public async Task GetBatchItemsAsync_FailObjectType_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableWithPrimaryKeyId, data);

            var query = new Query
            {
                Id = data.Id
            };

            await Assert.ThrowsAsync<IncorrectDataFormatException>(
                () =>
                    target.GetBatchItemsAsync<DateTime>(tableWithPrimaryKeyId, new[] {query}));
        }

        [Fact]
        public async Task UpdateItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableWithPrimaryKeyId, data);

            var update = new
            {
                Id = data.Id,
                Name = Guid.NewGuid().ToString()
            };

            await target.UpdateItemAsync(tableWithPrimaryKeyId, update);
        }

        [Fact]
        public async Task UpdateItemAsync_NotExistsItem_Test()
        {
            var update = new
            {
                Id = 0,
                Value = Guid.NewGuid().GetHashCode(),
                Name = Guid.NewGuid().ToString()
            };

            await Assert.ThrowsAsync<NotExistKeyException>(
                () =>
                    target.UpdateItemAsync(tableWithPrimaryKeyId, update));
        }

        [Fact]
        public async Task UpdateItemAsync_FailTableName_Test()
        {
            await Assert.ThrowsAsync<TableNameFailException>(
                () =>
                    target.UpdateItemAsync(Guid.NewGuid().ToString(), new {Id = 1}));
        }

        [Fact]
        public async Task UpdateItemAsync_FailKeyName_Test()
        {
            var data = new
            {
                Value = Guid.NewGuid().GetHashCode()
            };

            await Assert.ThrowsAsync<PrimaryKeyNameFailException>(
                () =>
                    target.UpdateItemAsync(tableWithPrimaryKeyId, data));
        }
    }
}