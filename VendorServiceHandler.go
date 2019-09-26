package main

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/micro/go-micro/util/log"
	"github.com/micro/protobuf/ptypes"
	TileService "github.com/nayanmakasare/TileService/proto"
	VendorService "github.com/nayanmakasare/VendorService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type VendorServiceHandler struct {
	MongoCollection *mongo.Collection
	RedisConnection  *redis.Client
}

type Subscriber struct{}

func (sub *Subscriber) Process(ctx context.Context, vendorInfo *TileService.GetRowsRequest) error {
	log.Info("Picked up a new message")
	log.Info("Got movie Tile in vendor Service ",vendorInfo.Vendor, vendorInfo.UserId, vendorInfo.Brand)
	return nil
}

func (h *VendorServiceHandler) GetVendorSpecification(ctx context.Context, req *VendorService.VendorRequestSpecification, stream VendorService.VendorService_GetVendorSpecificationStream ) error {
	log.Info("Get Ui Hit")
	var vendorBrandSpec VendorService.VendorBrandSpecification
	 err := h.MongoCollection.FindOne(ctx, bson.D{{"vendor", req.Vendor},{"brand", req.Brand},}).Decode(&vendorBrandSpec)
	 if err != nil {
		 return err
	 }
	 err = stream.Send(&vendorBrandSpec)
	 if err != nil {
		 return err
	 }
	 return stream.Close()
}

func (h *VendorServiceHandler) RegisterOrUpdateBrand(ctx context.Context, specification *VendorService.VendorBrandSpecification, response *VendorService.BrandResponse) error {
	singleResult  := h.MongoCollection.FindOne(ctx, bson.D{{"vendor", specification.Vendor},{"brand", specification.Brand},})
	ts, _ := ptypes.TimestampProto(time.Now())
	if singleResult.Err() != nil {
		specification.CreatedAt = ts
		_, err := h.MongoCollection.InsertOne(ctx, specification)
		if err != nil {
			log.Info("inserting 1")
			return err
		}
		response.ResponseMessage = "SuccessFully registered Vendor "+specification.Vendor+" and brand "+specification.Brand
		response.IsSuccessfull = true
		return nil
	}
	log.Info("Trigger 3")
	var tempVendorSpec VendorService.VendorBrandSpecification
	err := singleResult.Decode(&tempVendorSpec)
	log.Info(err)
	specification.CreatedAt = tempVendorSpec.CreatedAt
	specification.UpdatedAt = ts
	_, err = h.MongoCollection.ReplaceOne(ctx,bson.D{{"vendor", specification.Vendor},{"brand", specification.Brand},},specification)
	if err != nil {
		return err
	}
	response.ResponseMessage = "SuccessFully Updated Vendor "+specification.Vendor+" and brand "+specification.Brand
	response.IsSuccessfull = true
	return nil
}

func (h *VendorServiceHandler) UnRegisterBrand(ctx context.Context, specification *VendorService.VendorRequestSpecification, response *VendorService.BrandResponse) error {
	result := h.MongoCollection.FindOne(ctx, bson.D{{"vendor", specification.Vendor},{"brand", specification.Brand},})
	if result.Err() != nil {
		return result.Err()
	}
	_, err := h.MongoCollection.DeleteOne(ctx, bson.D{{"vendor", specification.Vendor},{"brand", specification.Brand},})
	if err != nil {
		log.Info("error while deleting")
		return err
	}
	response.ResponseMessage = "SuccessFully Unregistered Vendor "+specification.Vendor+" and brand "+specification.Brand
	response.IsSuccessfull = true
	return nil
}

//func (h *VendorServiceHandler) makeMovieRows(ctx context.Context, specification *VendorService.VendorBrandSpecification) {
//
//	log.Info("makeMovieRows 1")
//	options := options.Find()
//	// Sort by `_id` field descending
//	options.SetSort(bson.D{{"_id", -1}})
//	log.Info("makeMovieRows 2")
//	vendorRedisKey := specification.Vendor+":"+specification.Brand
//
//	for _, rowSpec := range specification.RowSpecification {
//		rowSpec.RowName = strings.Replace(rowSpec.RowName, " ", "-", -1)
//		rowVendorRedisKey := vendorRedisKey +":"+ rowSpec.RowPosition +":"+ rowSpec.RowName
//		h.RedisConnection.SAdd(vendorRedisKey, rowVendorRedisKey)
//		cur, err := h.MovieMongoCollection.Find(ctx,bson.D{{"source", bson.D{{"$in", bson.A{rowSpec.RowSource[0], rowSpec.RowSource[1]}}}}}, options)
//		if err != nil {
//			log.Info("makeMovieRows 3")
//			log.Info(err.Error())
//		}
//
//		for cur.Next(ctx) {
//			var resultMovie movieService.MovieTile
//			err := cur.Decode(&resultMovie)
//			if err != nil {
//				log.Info("makeMovieRows 4")
//				log.Info(err)
//			}
//			resultByteArray, err := proto.Marshal(&resultMovie)
//			if err != nil {
//				log.Info("makeMovieRows 5")
//				log.Info(err)
//			}
//			saddResult := h.RedisConnection.HSet(rowVendorRedisKey, resultMovie.Tid, resultByteArray)
//			if _,err = saddResult.Result(); err != nil {
//				log.Info("makeMovieRows 6")
//				log.Info(err)
//			}
//		}
//		log.Info(cur.Close(ctx))
//	}
//}









//func (h *VendorServiceHandler) callMovieService(specification *VendorService.VendorBrandSpecification) error {
//	var movieRowSpecs []*movieService.RowSpecification
//	for _, v := range specification.RowSpecification {
//		log.Info("nayan", v.RowName, v.RowShape, v.RowPosition, v.RowSource, v.ContentLanguage)
//		movieRowSpec := &movieService.RowSpecification{
//			RowSource:            v.RowSource,
//			ContentType:          v.ContentType,
//			RowName:              v.RowName,
//			RowPosition:          v.RowPosition,
//			RowShape:             v.RowShape,
//			ContentLanguage:	  v.ContentLanguage,
//		}
//		movieRowSpecs = append(movieRowSpecs, movieRowSpec)
//	}
//	log.Info(len(movieRowSpecs))
//	brandSpecification := &movieService.VendorBrandSpecification{
//		Vendor:           specification.Vendor,
//		Brand:            specification.Brand,
//		RowSpecification: movieRowSpecs,
//	}
//
//	_, err := h.MovieServiceClient.MakeMovieRows(context.TODO(), brandSpecification)
//	if err != nil {
//		return err
//	}
//	return nil
//}
