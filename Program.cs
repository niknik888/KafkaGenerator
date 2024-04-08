
using Confluent.Kafka;
using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string kafkaBootstrapServers = "192.168.108.253:9092"; // Адрес и порт Kafka брокера
        int i = 0;
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaBootstrapServers
        };

        // Создание объекта Producer
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (true)
            {
                Console.WriteLine("Press any key to send a message to Kafka, or press 'q' to quit:");
                var key = Console.ReadKey(intercept: true).Key;

                if (key == ConsoleKey.Q)
                    break;


                try
                {
                    string topicName = "Test.VrbParameters"; // Название топика, в который отправляем сообщения


                    for (i = 1; i <= 3; i++)
                    {
                        var objId = i;
                        var parameterid = i;
                        var value = i * 47.9;
                        string message =
                        $"{{\r\n  \"VrbId\": 1_22_33_{objId},\r\n  " +
                        $"\"ParameterId\": 2115,\r\n  " +
                        $"\"Timestamp\": {DateTime.Now },\r\n  " +
                        $"\"Timezoneoffset\": 3,\r\n  " +
                        $"\"Value\": {value},\r\n  ";

                        var deliveryResult = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = message });
                        Console.WriteLine($"Delivered message '{message}' to '{deliveryResult.TopicPartitionOffset}'");
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
