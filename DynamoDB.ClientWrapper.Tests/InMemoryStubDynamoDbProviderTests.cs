namespace DynamoDB.ClientWrapper.Tests
{
    using FluentAssertions;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Xunit;

    public class InMemoryStubDynamoDbProviderTests
    {
        private readonly InMemoryStubDynamoDbProvider target;
        private const string tableName = "Test";

        public InMemoryStubDynamoDbProviderTests()
        {
            target = new InMemoryStubDynamoDbProvider(
                tableName,
                new[] {"Id"},
                new Dictionary<string, string>());
        }

        [Fact]
        public async Task PutItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableName, data);
        }

        [Fact]
        public async Task PutItemAsync_FailDuplicate_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableName, data);

            await Assert.ThrowsAsync<DuplicateKeyException>(
                () =>
                    target.PutItemAsync(tableName, data, true));
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
                    target.PutItemAsync(tableName, data));
        }

        [Fact]
        public async Task GetBatchItemsAsync_CheckEmptyResult_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            var responce = await target.GetBatchItemsAsync<TestData>(tableName, new[] {Query.Empty});

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

            await target.PutItemAsync(tableName, data);

            var query = new Query
            {
                Id = data.Id
            };

            var responce = await target.GetBatchItemsAsync<TestData>(tableName, new[] {query});

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
        public async Task GetBatchItemsAsync_FailObjectType_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableName, data);

            var query = new Query
            {
                Id = data.Id
            };

            await Assert.ThrowsAsync<IncorrectDataFormatException>(
                () =>
                    target.GetBatchItemsAsync<DateTime>(tableName, new[] {query}));
        }


        [Fact]
        public async Task UpdateItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };

            await target.PutItemAsync(tableName, data);

            var update = new
            {
                Id = data.Id,
                Name = Guid.NewGuid().ToString()
            };

            await target.UpdateItemAsync(tableName, update);
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
                    target.UpdateItemAsync(tableName, update));
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
                    target.UpdateItemAsync(tableName, data));
        }
    }
}