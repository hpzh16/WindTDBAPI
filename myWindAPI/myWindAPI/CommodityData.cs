using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TDBAPI;
using System.Data.SqlClient;
using System.Data;

namespace myWindAPI
{
    class CommodityData
    {
        public string orignalConnectString = "server=(local);database=;Integrated Security=true;";
        public TDBsource mySource = new TDBsource("114.80.154.34", "10060", "TD5928909014", "13305104");
        public List<commodityFormat> commodityList = new List<commodityFormat>();
        public string market;
        public int startDate;
        public int endDate;

        /// <summary>
        /// TDB数据接口类。
        /// </summary>
        private TDBDataSource tdbSource;

        /// <summary>
        /// 记录交易日信息的类。
        /// </summary>
        public TradeDays myTradeDays;

        public CommodityData(string market,int startDate,int endDate=0)
        {
            this.market = market.ToUpper();
            this.startDate = startDate;
            if (endDate==0)
            {
                endDate = startDate;
            }
            this.endDate = endDate;
            //对接口类进行初始化。
            tdbSource = new TDBDataSource(mySource.IP, mySource.port, mySource.account, mySource.password);
            if (CheckConnection())
            {
                Console.WriteLine("Connect Success!");
                myTradeDays = new TradeDays(startDate, endDate);
                Console.WriteLine("Tradedays Collect!");
                commodityList = GetCommodityList(market);
                Console.WriteLine("CommodityList Collect!");
                StoreData();
            }
            else
            {
                Console.WriteLine("Please Input Valid Parameters!");
            }

            //工作完毕之后，关闭万德TDB数据库的连接。
            //关闭连接
            tdbSource.DisConnect();
        }

        public void StoreData()
        {
            foreach (var commodity in commodityList)
            {
                string tableName = "MarketData_" + commodity.contractName + "_" + commodity.market;
                foreach (int today in myTradeDays.myTradeDays)
                {
                    TDBReqFuture reqFuture = new TDBReqFuture(commodity.contractName,today,today);
                    TDBFutureAB[] futureABArr;
                    TDBErrNo nErrInner = tdbSource.GetFutureAB(reqFuture, out futureABArr);
                    if (futureABArr.Length>0)
                    {
                        string todayDataBase = "TradeMarket" + (today / 100).ToString();
                        string yesterdayDataBase = "TradeMarket"+(TradeDays.GetPreviousTradeDay(today)/100).ToString();
                        if (SqlApplication.CheckDataBaseExist(todayDataBase,orignalConnectString)==false)
                        {
                            CreateDataBase(todayDataBase, orignalConnectString);
                        }
                    }
                }
            }
            
        }

        public void CreateDataBase(string dataBaseName,string connectString)
        {
            using (SqlConnection conn = new SqlConnection(connectString))
            {
                conn.Open();//打开数据库  
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE DATABASE "+dataBaseName+" ON PRIMARY (NAME = '"+dataBaseName+"', FILENAME = 'E:\\"+dataBaseName+ ".dbf',SIZE = 1024MB,MaxSize = 512000MB,FileGrowth = 1024MB) LOG ON (NAME = '" + dataBaseName + "Log',FileName = 'E:\\" + dataBaseName + ".ldf',Size = 20MB,MaxSize = 1024MB,FileGrowth = 10MB)";
                try
                {
                    cmd.ExecuteReader();
                }
                catch (Exception myerror)
                {
                    System.Console.WriteLine(myerror.Message);
                }
            }
        }

        /// <summary>
        /// 获取商品期货列表的信息。
        /// </summary>
        /// <param name="market">市场</param>
        /// <returns>商品期货信息列表</returns>
        public List<commodityFormat> GetCommodityList(string market)
        {
            List<commodityFormat> myList = new List<commodityFormat>();
            TDBCode[] codeArr;
            tdbSource.GetCodeTable(market, out codeArr);
            foreach (var item in codeArr)
            {
                commodityFormat myCommodity = new commodityFormat();
                string code = item.m_strCode.ToUpper();
                string date="";
                myCommodity.contractName = code;
                if (market=="CZC")
                {
                    date = MyApplication.RemoveNotNumber(code);
                    date = "1" + date;
                }
                else
                {
                    date = MyApplication.RemoveNotNumber(code);
                }
                if (date.Length!=4)
                {
                    continue;
                }
                if (date.Length==4)
                {
                    int num = Convert.ToInt32(date);
                    if (num>=1304)
                    {
                        myCommodity.contractName = code+"."+market;
                        myCommodity.code = code;
                        myCommodity.startDate = (num - 100+200000) * 100 + 01;
                        myCommodity.endDate = (num+200000) * 100 + 31;
                        myCommodity.market = market;
                        myList.Add(myCommodity);
                    }
                }
            }
            return myList;
        }

        /// <summary>
        /// 判断TDB数据库是否连接成功。
        /// </summary>
        /// <returns>返回是否连接成功。</returns>
        public bool CheckConnection()
        {
            TDBLoginResult loginRes;
            TDBErrNo nErr = tdbSource.Connect(out loginRes);
            //输出登陆结果
            if (nErr == TDBErrNo.TDB_OPEN_FAILED)
            {
                Console.WriteLine("open failed, reason:{0}", loginRes.m_strInfo);
                Console.WriteLine();
                return false;
            }
            return true;
        }
    }
}
