package main

import (
	"bytes"
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
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              *sync.Mutex
	loop            *bool
	currentPid      uint64 = 0
	version                = "0.01"
	baseDir                = "store"
	dataDir         string
	sxServerAddress string
	mbusID          uint64                  = 0 // storage MBus ID
	pc_client       *sxutil.SXServiceClient = nil
	minioClient     *minio.Client
)

func init() {
	endpoint := "127.0.0.1:9000" // just use local minio for test.
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

	log.Printf("Connected to Minio Client %v, %#v\n", err, minioClient) // minioClient is now set up

	buckets, berr := minioClient.ListBuckets(context.Background())
	if berr != nil {
		log.Println("Cant get buckets", berr)
		return
	}
	log.Printf("Bucket %d", len(buckets))
	for n, bucket := range buckets {
		log.Printf("%2d:%s,", n, bucket)
	}

}

func mbusCallback(clt *sxutil.SXServiceClient, mm *api.MbusMsg) {
	log.Printf("Mbus message %#v", mm)

	if sxutil.IDType(mm.TargetId) == clt.ClientID { // this msg is for me.
		if mm.MsgInfo == "Store" {
			record := &storage.Record{}
			err := proto.Unmarshal(mm.Cdata.Entity, record)
			if err == nil {
				minioClient.PutObject(context.Background(), record.BucketName, record.ObjectName, bytes.NewBuffer(record.Record), int64(len(record.Record)), minio.PutObjectOptions{})
				log.Printf("Store bucket %s, objname %s , len %d", record.BucketName, record.ObjectName, len(record.Record))
			}
		}
	}

}

func selectSupplyCB(clt *sxutil.SXServiceClient, dm *api.Demand) {
	log.Printf("Current Supply Selected! %v", dm)
	err := clt.Confirm(sxutil.IDType(dm.Id)) // send confirm to sender!
	if err == nil {
		log.Printf("Confirm success!")
		go clt.SubscribeMbus(context.Background(), mbusCallback)
	} else {
		log.Printf("Confirm error %v", err)
	}
}

func notifyDemandCB(clt *sxutil.SXServiceClient, dm *api.Demand) {
	if dm.DemandName == "Storage" {
		storageInfo := &storage.Storage{}
		err := proto.Unmarshal(dm.Cdata.Entity, storageInfo)
		log.Printf("Receive Storage Demand! %v", storageInfo)
		if err == nil { /// lets start subscribe pcounter.
			if storageInfo.Stype == storage.StorageType_TYPE_OBJSTORE && storageInfo.Dtype == storage.DataType_DATA_FILE {
				log.Printf("Type OK")

				cdata := api.Content{Entity: dm.Cdata.Entity}
				spo := sxutil.SupplyOpts{
					Target: dm.Id,
					Name:   "Storage",
					Cdata:  &cdata,
				}
				currentPid = clt.ProposeSupply(&spo)
				log.Printf("Propose Supply %d, %v", currentPid, spo)
			} else {
				log.Printf("I can't support this storage type.")
			}
		} else {
			log.Printf("Unmarshaling err %v", err)
		}
	}
}

/*
func supplyStorageCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	//	log.Printf("Receive Callback: %v", dm)
	if currentPid != 0 && currentPid == dm.TargetId { // select supply!!
		log.Printf("Current Supply Selected! %v", dm)
		err := clt.Confirm(sxutil.IDType(dm.Id)) // send confirm to sender!
		if err == nil {
			log.Printf("Confirm success!")
			go clt.SubscribeMbus(context.Background(), mbusCallback)
		} else {
			log.Printf("Confirm error %v", err)
		}
	} else if dm.DemandName == "Storage" {
		storageInfo := &storage.Storage{}
		err := proto.Unmarshal(dm.Cdata.Entity, storageInfo)
		log.Printf("Receive Storage Demand! %v", storageInfo)
		if err == nil { /// lets start subscribe pcounter.
			if dm.TargetId == 0 { // notifyDemand
				//				log.Printf("Receive Notify Demand! %v", storageInfo)

				if storageInfo.Stype == storage.StorageType_TYPE_OBJSTORE && storageInfo.Dtype == storage.DataType_DATA_FILE {
					log.Printf("Type OK")

					cdata := api.Content{Entity: dm.Cdata.Entity}
					spo := sxutil.SupplyOpts{
						Target: dm.Id,
						Name:   "Storage",
						Cdata:  &cdata,
					}
					currentPid = clt.ProposeSupply(&spo)
					log.Printf("Propose Supply %d, %v", currentPid, spo)
				}
			} else {
				log.Printf("Why this demand? %v", dm)
			}
		} else {
			log.Printf("Unmarshaling err %v", err)
		}
	}

}
*/

type myDemandHandler struct{}

func (dh myDemandHandler) OnNotifyDemand(clt *sxutil.SXServiceClient, dm *api.Demand) *sxutil.SupplyOpts {
	if dm.DemandName == "Storage" {
		storageInfo := &storage.Storage{}
		err := proto.Unmarshal(dm.Cdata.Entity, storageInfo)
		if err == nil { /// lets start subscribe pcounter.
			log.Printf("Receive Storage Demand! %v", storageInfo)
			if storageInfo.Stype == storage.StorageType_TYPE_OBJSTORE && storageInfo.Dtype == storage.DataType_DATA_FILE {
				log.Printf("Type OK")
				cdata := api.Content{Entity: dm.Cdata.Entity}
				spo := sxutil.SupplyOpts{
					Name:  "Storage",
					Cdata: &cdata,
				}
				return &spo // send "ProposeSupply" in handler
			} else {
				log.Printf("I can't support this storage type.")
			}
		} else {
			log.Printf("Unmarshaling err %v", err)
		}
	}
	return nil
}
func (dh myDemandHandler) OnSelectSupply(clt *sxutil.SXServiceClient, dm *api.Demand) bool {
	log.Printf("OnSelectSupply called! %v", dm)
	// always OK
	return true
}

func (dh myDemandHandler) OnConfirmResponse(clt *sxutil.SXServiceClient, dm *api.Demand, err error) {
	log.Printf("Confirm responsed! %v", err)
	if err == nil {
		go clt.SubscribeMbus(context.Background(), mbusCallback)
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
		return
	}

	st_client := sxutil.NewSXServiceClient(client, pbase.STORAGE_SERVICE, "{Client:PCObjStore}")
	log.Print("Subscribe Storage Demand")

	//	mu, loop = sxutil.SimpleSubscribeDemand(st_client, supplyStorageCallback)

	mu, loop = sxutil.CombinedSubscribeDemand(st_client, notifyDemandCB, selectSupplyCB)

	wg.Add(1)
	//	go subscribePCounterSupply(pc_client)

	wg.Wait()

}
