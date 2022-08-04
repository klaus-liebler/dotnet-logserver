using System.Net;
using System.Text;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

namespace Logserver
{
    public class LogFileDescriptor{
        public DateOnly creationDay{get;}
        public System.IO.FileStream file{get;}

        public LogFileDescriptor(DateOnly creationDay, System.IO.FileStream file){
            this.creationDay=creationDay;
            this.file=file;
        }
    };

    class Binlog2Serilog{
        private readonly Microsoft.Extensions.Logging.ILogger<Binlog2Serilog> LOG;
        private readonly Dictionary<string, LogFileDescriptor> host2file = new();
        private static Semaphore semaphore=new(1,1);
        public Binlog2Serilog(Microsoft.Extensions.Logging.ILogger<Binlog2Serilog> logger){
            LOG=logger;
        }

        private string severity2string(int severity){
            switch (severity)
            {
                case 0: return "FATAL";
                case 1: return "FATAL";
                case 2: return "FATAL";
                case 3: return "ERROR";
                case 4: return "WARN";
                case 5: return "INFO";
                case 6: return "INFO";
                case 7: return "DEBUG";
                default: return "FATAL";
            }
        }

        public void ReceiveCallback(IAsyncResult ar)
        {
            UdpClient u = (ar.AsyncState as UdpClient)!;
            IPEndPoint otherStation=new IPEndPoint(IPAddress.Any, 514);
            byte[] receiveBytes = u.EndReceive(ar, ref otherStation!);
            LogFileDescriptor desc;
            semaphore.WaitOne();
            string host = otherStation.Address.ToString();
            if(!host2file.TryGetValue(host, out desc)){
                desc=new(DateOnly.FromDateTime(DateTime.Now), new FileStream("log-"+host+".log", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read));
                host2file.Add(host, desc);
            }
            Int64 timestamp = BitConverter.ToInt64(receiveBytes, 0);
            string localTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            Int64 numericValue1 = BitConverter.ToInt64(receiveBytes, 8);
            Int64 numericValue2 = BitConverter.ToInt64(receiveBytes, 16);
            string severity = severity2string(receiveBytes[24]);
            string wholeString = Encoding.ASCII.GetString(receiveBytes, 28, receiveBytes.Length-28);
            var strings =  wholeString.Split(new char[] {'\0'}, StringSplitOptions.RemoveEmptyEntries);
            string tag = strings[0];
            string message = strings[1];

            string value = $"{localTime} {severity} {host} {tag} {numericValue1} {numericValue2} {message}";
            Console.WriteLine($"Received: {value}");
            byte[] info = new UTF8Encoding(true).GetBytes(value+"\n");
            desc.file.Write(info, 0, info.Length);
            desc.file.Flush(); 
            semaphore.Release();
            u.BeginReceive(new AsyncCallback(ReceiveCallback), u);
        }

        public void InitAndRun(){
            IPEndPoint e = new IPEndPoint(IPAddress.Any, 514);
            UdpClient u = new UdpClient(e);
            u.BeginReceive(new AsyncCallback(ReceiveCallback), u);
        }

        public void Close(){
            semaphore.WaitOne();
            foreach (var item in host2file.Values)
            {
                item!.file.Close();
            }
            host2file.Clear();
            semaphore.Release();
        }
    }


    class Program
    {
      

        private static void ConfigureServices(IServiceCollection services)
        {
            services
                .AddLogging(configure => configure.AddSerilog())
                .AddTransient<Binlog2Serilog>();
        }

        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().WriteTo.File("consoleapp.log").CreateLogger();
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            var serviceProvider = serviceCollection.BuildServiceProvider();   

            var logger = serviceProvider.GetService<ILogger<Program>>();

            

            var udplog = serviceProvider.GetService<Binlog2Serilog>()!;
            Console.WriteLine("listening for messages");
            udplog.InitAndRun();
            Console.WriteLine("Press any key to finish");
            Console.ReadKey();
            Console.WriteLine("Closing all files");
            udplog.Close();
            Console.WriteLine("Exiting");
        }
    }
}