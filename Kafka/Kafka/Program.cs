
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka
{
    class Program
    {
        private const string TopicName = "users";
        private const string BootstrapServers = "localhost:9093";
        public static async Task Main(string[] args)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = BootstrapServers
            };

            using var adminClient = new AdminClientBuilder(config)
                                   .Build();
            //удаление топика
            /* IEnumerable<string> topicList = new List<string>() { "users" };
             adminClient.DeleteTopicsAsync(topicList,null);*/

            //создание топика
            adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = TopicName, ReplicationFactor = 1, NumPartitions = 1 } });


            //вывод топиков
            using (var adminClient2 = new AdminClientBuilder(config).Build())
            {
                var metadata = adminClient2.GetMetadata(TimeSpan.FromSeconds(10));
                var topicsMetadata = metadata.Topics;
                var topicNames = metadata.Topics.Select(a => a.Topic).ToList();
                Console.WriteLine("Топики:");
                foreach (var s in topicNames)
                {
                    Console.WriteLine(s);
                }
            }

            //запись сообщений
            var config2 = new ProducerConfig { BootstrapServers = BootstrapServers };
            using (var p = new ProducerBuilder<Null, string>(config2).Build())
            {
                Console.WriteLine("Чтобы закончить ввод, нажмите Esc");
                ConsoleKeyInfo keyKode = Console.ReadKey();
                Console.WriteLine("Ввод сообщения: ");
                do
                {
                    try
                    {
                        var dr = await p.ProduceAsync(TopicName, new Message<Null, string> { Value = Console.ReadLine() });
                        Console.WriteLine($"Доставлено '{dr.Value}' для '{dr.TopicPartitionOffset}'");
                        keyKode = Console.ReadKey(true);
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Доставка не удалась: {e.Error.Reason}");
                    }
                } while (keyKode.Key != ConsoleKey.Escape);
            }


            //чтение сообщений
            Console.WriteLine("Сообщения для пользователя:");

            var consumerConfig = new ConsumerConfig

            {

                BootstrapServers = BootstrapServers,

                GroupId = "group1",

                AutoOffsetReset = AutoOffsetReset.Earliest

            };
            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

            consumer.Subscribe(TopicName);

            try
            {
                while (true)
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (result == null)
                    {
                        continue;
                    }
                    Console.WriteLine($"Сообщение '{result.Message.Value}': '{result.TopicPartitionOffset}'.");
                }
            }

            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка: {ex.Message}");
            }
            Console.ReadLine();
        }
    }
}