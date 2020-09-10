package main

import (
	"context"
	"flag"

	"github.com/golang/protobuf/proto"
	storage "github.com/synerex/proto_storage"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"log"
	"sync"
	"time"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	sxServerAddress string
	mbusID          int64                   = -1 // storage MBus ID
	pc_client       *sxutil.SXServiceClient = nil
	minioClient     *minio.Client
)

func init() {
	endpoint := "127.0.0.1"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false
	// Initialize minio client object.
	var err error
	minioClient, err = minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Connected to Minio Client %#v\n", minioClient) // minioClient is now set up
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func mbusCallback(clt *sxutil.SXServiceClient, mm *api.MbusMsg) {
	log.Printf("Mbus message %#v", mm)
}

func supplyStorageCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	storageInfo := &storage.Storage{}
	log.Printf("Demand! %v", dm)

	if dm.DemandName == "Storage" {
		err := proto.Unmarshal(dm.Cdata.Entity, storageInfo)
		if err == nil { /// lets start subscribe pcounter.
			if dm.TargetId == 0 { // notifyDemand
				log.Printf("Notify Demand! %v", storageInfo)

				if storageInfo.Stype == storage.StorageType_TYPE_OBJSTORE && storageInfo.Dtype == storage.DataType_DATA_FILE {
					log.Printf("Type OK")

					cdata := api.Content{Entity: dm.Cdata.Entity}
					spo := sxutil.SupplyOpts{
						Target: dm.Id,
						Name:   "Storage",
						Cdata:  &cdata,
					}
					clt.ProposeSupply(&spo)
					log.Printf("Propose Supply %v", spo)
				}
			} else if dm.TargetId == uint64(clt.ClientID) { // selected!
				clt.Confirm(sxutil.IDType(dm.SenderId)) // send confirm to sender!
				// now mbus id was set!
				log.Printf("selected! send confirm %v", dm)

				go clt.SubscribeMbus(context.Background(), mbusCallback)
			}
		}
	}

}

func subscribeStorageDemand(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeDemand(ctx, supplyStorageCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("ObjStorage-MINIO(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.STORAGE_SERVICE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "ObjStorageMinio", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	st_client := sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:PCObjStore}")

	log.Print("Subscribe Storage Demand")
	go subscribeStorageDemand(st_client)

	storageInfo := storage.Storage{
		Stype: storage.StorageType_TYPE_OBJSTORE,
		Dtype: storage.DataType_DATA_FILE,
	}

	out, err := proto.Marshal(&storageInfo)
	if err == nil {
		cont := api.Content{Entity: out}
		// Register supply
		dmo := sxutil.DemandOpts{
			Name:  "Storage",
			Cdata: &cont,
		}
		//			fmt.Printf("Res: %v",smo)
		//_, _ :=
		st_client.NotifyDemand(&dmo)
	}

	wg.Add(1)
	//	go subscribePCounterSupply(pc_client)

	wg.Wait()

}
