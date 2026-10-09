package main

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "time"
 "github.com/nats-io/nats.go"
)
func main() {
 var config struct { URLs []string; Subject string; Request json.RawMessage }
 body,err:=os.ReadFile(os.Args[1]);if err!=nil{panic(err)}
 if err=json.Unmarshal(body,&config);err!=nil{panic(err)}
 for _,url:=range config.URLs {
  nc,err:=nats.Connect(url,nats.NoReconnect()); if err!=nil{fmt.Println(url,err);continue}
  for _,request:=range []struct{subject string;body []byte}{{config.Subject,config.Request},{"$JS.ARCHIVE.API.STREAM.INFO.OBJ_ARCHIVE_OBJECTS",nil}} {
   ctx,cancel:=context.WithTimeout(context.Background(),time.Second)
   start:=time.Now();msg,err:=nc.RequestWithContext(ctx,request.subject,request.body);cancel()
   if msg!=nil {fmt.Printf("url=%s subject=%s elapsed=%s error=%v response=%s\n",url,request.subject,time.Since(start),err,msg.Data)} else {fmt.Printf("url=%s subject=%s elapsed=%s error=%v\n",url,request.subject,time.Since(start),err)}
  }
  nc.Close()
 }
}
