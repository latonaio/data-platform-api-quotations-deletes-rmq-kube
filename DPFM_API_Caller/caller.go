package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-quotations-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-quotations-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-quotations-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	switch input.APIType {
	case "deletes":
		response = c.deleteSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var headerData *dpfm_api_output_formatter.Header
	itemData := make([]dpfm_api_output_formatter.Item, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s := c.headerDelete(input, output, log)
			headerData = h
			if h == nil || i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
		case "Item":
			i, s := c.itemDelete(input, output, log)
			if i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
	}

	return &dpfm_api_output_formatter.Message{
		Header:       headerData,
		Item:         &itemData,
	}
}

func (c *DPFMAPICaller) headerDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Item) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderRead(input, log)
	if header == nil {
		return nil, nil, nil
	}
	header.IsMarkedForDeletion = input.Quotations.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "QuotationsHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot delete"
		return nil, nil, nil
	}

	// headerの削除が取り消された時は子に影響を与えない
	if !*header.HeaderIsDeleted {
		return header, nil, nil
	}

	items := c.ItemsRead(input, log)
	for i := range *items {
		(*items)[i].IsMarkedForDeletion = input.Quotations.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*items)[i], "function": "QuotationsItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Quotation Item Data cannot delete"
			return nil, nil, nil
		}
	}

	return header, items
}

func (c *DPFMAPICaller) itemDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ScheduleLine) {
	sessionID := input.RuntimeSessionID
	items := make([]dpfm_api_output_formatter.Item, 0)
	for _, v := range input.Quotations.Item {
		data := dpfm_api_output_formatter.Item{
			Quotation:          	input.Quotations.Quotation,
			QuotationItem:      	v.Quotation,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "QuotationsItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Quotation Item Data cannot delete"
			return nil, nil
		}
	}

	// itemがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Quotations.Item[0].IsMarkedForDeletion {
		header := c.HeaderRead(input, log)
		header.IsMarkedForDeletion = input.Quotations.Item[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "QuotationsHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot delete"
			return nil, nil
		}
	}

	return &items
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
