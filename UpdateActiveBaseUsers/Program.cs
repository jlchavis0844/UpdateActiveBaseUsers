using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using unirest_net.http;

namespace UpdateActiveBaseUsers {
    class Program {
        public static string token = "";
        public static string connString = "Data Source=RALIMSQL1;Initial Catalog=CAMSRALFG;Integrated Security=SSPI;";


        static void Main(string[] args) {
            var fs = new FileStream(@"C:\apps\NiceOffice\token", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            using (var sr = new StreamReader(fs)) {
                token = sr.ReadToEnd();
            }

            UpdateUsers();
        }

        public static void UpdateUsers() {
            List<Object[]> updatedUsers = new List<Object[]>();
            string baseURL = "https://api.getbase.com/v2/users?per_page=100&status=active";
            string rawJSON = Get(baseURL, token);
            JObject jsonObj = JObject.Parse(rawJSON) as JObject;
            JArray jArr = jsonObj["items"] as JArray;
            
            foreach(var obj in jArr) {
                var data = obj["data"];

                int id = Convert.ToInt32(data["id"]);//0
                string name = data["name"].ToString();//1
                string email = data["email"].ToString();//2

                string phone_number = "";//3
                if(data["phone_number"] != null && data["phone_number"].ToString() != "") {
                    phone_number = data["phone_number"].ToString();
                }

                string time_zone = "";//4
                if (data["time_zone"] != null && data["time_zone"].ToString() != "") {
                    time_zone = data["time_zone"].ToString();
                }

                DateTime created_at = Convert.ToDateTime(data["created_at"]).ToLocalTime();//5
                DateTime updated_at = Convert.ToDateTime(data["updated_at"]).ToLocalTime();//6

                string role = "Unknown";//7
                if (data["role"] != null && data["role"].ToString() != "") {
                    role = data["role"].ToString();
                }

                string status = "Unknown";//8
                if (data["status"] != null && data["status"].ToString() != "") {
                    status = data["status"].ToString();
                }

                bool invited = false;//9
                if(data["invited"] != null && data["invited"].ToString() != "") {
                    invited = Convert.ToBoolean(data["invited"]);
                }

                string team_name = "Unknown";//10
                if (data["team_name"] != null && data["team_name"].ToString() != "") {
                    team_name = data["team_name"].ToString();
                }

                int group_id = 0;//11
                string group_name = "None";//12

                JObject group = data["group"] as JObject;
                if (group != null && group.HasValues) {
                    group_id = Convert.ToInt32(group["id"]);
                    group_name = group["name"].ToString();
                }

                int reports_to = 0;//13
                if(data["reports_to"] != null && data["reports_to"].ToString() != "") {
                    reports_to = Convert.ToInt32(data["reports_to"]);
                }
                //                 0    1     2          3          4          5        6        7        
                Object[] tArr = { id, name, email, phone_number,time_zone,created_at,updated_at,role,
                    // 8      9        10       11       12          13
                    status,invited,team_name,group_id,group_name,reports_to};
                updatedUsers.Add(tArr);
            }
            SendUpdates(updatedUsers);
        }

        private static void ClearUsers(List<Object[]> myList) {
            string sqlStr = "DELETE FROM[CAMSRALFG].[dbo].[Base_ActiveUsers] WHERE [CAMSRALFG].[dbo].[Base_ActiveUsers].[id] is not null";
            using (SqlConnection connection = new SqlConnection(connString)) {
                SqlCommand delCommand = new SqlCommand(sqlStr, connection);
                try {
                    connection.Open();

                    int result = delCommand.ExecuteNonQuery();

                    if (result == 0) {
                        //log.WriteLine("INSERT failed for " + command.ToString());
                        //log.Flush();
                        Console.WriteLine("No Lines deleted from active table " + delCommand.ToString());
                    }
                }
                catch (Exception ex) {
                    //log.WriteLine(ex);
                    //log.Flush();
                    Console.WriteLine(ex);
                }
                finally {
                    connection.Close();
                }
            }
        }

        private static void SendUpdates(List<Object[]> myList) {
            string sqlStr = "INSERT INTO [Base_ActiveUsers] ([id],[name],[email],[phone_number],[timezone],[created_at],[updated_at]," +
                "[role],[status],[invited],[team_Name],[group_id],[group_name],[reports_to]) VALUES (" +
                // 0    1     2          3           4           5          6        7      8      9         10         11         12          13            
                "@id,@name,@email,@phone_number,@timezone,@created_at,@updated_at,@role,@status,@invited,@team_Name,@group_id,@group_name,@reports_to);";
            using (SqlConnection connection = new SqlConnection(connString)) {
                ClearUsers(myList);
                foreach (Object[] user in myList) {
                    using (SqlCommand command = new SqlCommand(sqlStr, connection)) {
                        command.Parameters.Add("@id", SqlDbType.Int).Value = user[0];
                        command.Parameters.Add("@name", SqlDbType.NVarChar).Value = user[1];
                        command.Parameters.Add("@email", SqlDbType.NVarChar).Value = user[2];
                        command.Parameters.Add("@phone_number", SqlDbType.NVarChar).Value = user[3];
                        command.Parameters.Add("@timeZone", SqlDbType.NVarChar).Value = user[4];
                        command.Parameters.Add("@created_at", SqlDbType.DateTime).Value = user[5];
                        command.Parameters.Add("@updated_at", SqlDbType.DateTime).Value = user[6];
                        command.Parameters.Add("@role", SqlDbType.NVarChar).Value = user[7];
                        command.Parameters.Add("@status", SqlDbType.NVarChar).Value = user[8];
                        command.Parameters.Add("@invited", SqlDbType.Bit).Value = user[9];
                        command.Parameters.Add("@team_Name", SqlDbType.NVarChar).Value = user[10];
                        command.Parameters.Add("@group_id", SqlDbType.Int).Value = user[11];
                        command.Parameters.Add("@group_name", SqlDbType.NVarChar).Value = user[12];
                        command.Parameters.Add("@reports_to", SqlDbType.Int).Value = user[13];

                        try {
                            connection.Open();

                            int result = command.ExecuteNonQuery();

                            if (result == 0) {
                                //log.WriteLine("INSERT failed for " + command.ToString());
                                //log.Flush();
                                Console.WriteLine("INSERT failed for " + command.ToString());
                            }
                        }
                        catch (Exception ex) {
                            //log.WriteLine(ex);
                            //log.Flush();
                            Console.WriteLine(ex);
                        }
                        finally {
                            connection.Close();
                        }
                    }
                }
            }
        }


        public static string Get(string url, string myToken) {
            string body = "";
            try {
                HttpResponse<string> jsonReponse = Unirest.get(url)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + myToken)
                    .asJson<string>();
                body = jsonReponse.Body.ToString();
                return body;
            }
            catch (Exception ex) {
                //log.WriteLine(ex);
                //log.Flush();
                Console.WriteLine(ex);
                return body;
            }
        }


    }
}
