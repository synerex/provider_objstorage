module objstorage

go 1.14

require (
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.5
	github.com/shirou/gopsutil v2.20.8+incompatible // indirect
	github.com/synerex/proto_pcounter v0.0.6
	github.com/synerex/proto_storage v0.0.2
	github.com/synerex/synerex_api v0.4.2
	github.com/synerex/synerex_proto v0.1.9
	github.com/synerex/synerex_sxutil v0.5.2
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	golang.org/x/sys v0.0.0-20200917073148-efd3b9a0ff20 // indirect
	google.golang.org/genproto v0.0.0-20200917134801-bb4cff56e0d0 // indirect
	google.golang.org/grpc v1.32.0 // indirect
	gopkg.in/ini.v1 v1.61.0 // indirect
//	local.packages/synerex_sxutil v0.4.12
)

//replace local.packages/synerex_sxutil => ../../synerex_beta/sxutil
