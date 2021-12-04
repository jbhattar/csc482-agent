package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	loggly "github.com/jamespearly/loggly"
	//"strconv"
)

type Results struct {
	Cache   Cache     `json:"cache"`
	RawData []RawData `json:"rawData"`
}
type Cache struct {
	LastUpdated          string `json:"lastUpdated"`
	Expires              string `json:"expires"`
	LastUpdatedTimestamp int64  `json:"lastUpdatedTimestamp"`
	ExpiresTimestamp     int64  `json:"expiresTimestamp"`
}

type RawData struct {
	FIPS              string `json:"FIPS"`
	Admin2            string `json:"Admin2"`
	ProvinceState     string `json:"Province_State"`
	CountryRegion     string `json:"Country_Region"`
	LastUpdate        string `json:"Last_Update"`
	Lat               string `json:"Lat"`
	Long              string `json:"Long_"`
	Confirmed         string `json:"Confirmed"`
	Deaths            string `json:"Deaths"`
	Recovered         string `json:"Recovered"`
	Active            string `json:"Active"`
	CombinedKey       string `json:"Combined_Key"`
	IncidentRate      string `json:"Incident_Rate"`
	CaseFatalityRatio string `json:"Case_Fatality_Ratio"`
}

func insertRawData(rawData []RawData) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	// insert rawdata into db
	for i := 0; i < 50; i++ {
		av, err := dynamodbattribute.MarshalMap(rawData[i])

		if err != nil {
			fmt.Println("Got error marshalling map:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("jbhattar-covid-19-data"),
		}

		_, err = svc.PutItem(input)

		if err != nil {
			fmt.Println("Got error calling PutItem:")
			fmt.Println(err.Error())
			os.Exit(1)
		}

		fmt.Println("Successfully added rawData")
	}
	fmt.Println("Successfully added full DATA")
}
func main() {

	tag := "My-Go-Demo"
	//Instantiate the client
	client := loggly.New(tag)

	ticker := time.NewTicker(time.Hour * 3)

	for ; true; <-ticker.C {

		resp, err := http.Get("http://coronavirus.m.pipedream.net/")
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)

		var results Results
		_err := json.Unmarshal(body, &results)

		if _err != nil {
			panic(_err)
		}
		client.EchoSend("info", "The coronavirus data is collected from pripdream")
		insertRawData(results.RawData)
		fmt.Println("Done....")
		client.EchoSend("info", "The coronavirus data is stored at jbhattar's Dynamo DB")

	}

}
