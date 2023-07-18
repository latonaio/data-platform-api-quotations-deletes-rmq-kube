package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-orders-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-orders-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-orders-deletes-rmq-kube/config"

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
	scheduleData := make([]dpfm_api_output_formatter.ScheduleLine, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s := c.headerDelete(input, output, log)
			headerData = h
			if h == nil || i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			scheduleData = append(scheduleData, *s...)
		case "Item":
			i, s := c.itemDelete(input, output, log)
			if i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			scheduleData = append(scheduleData, *s...)
		case "Schedule":
			s := c.scheduleDelete(input, output, log)
			if s == nil {
				continue
			}
			scheduleData = append(scheduleData, *s...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:       headerData,
		Item:         &itemData,
		ScheduleLine: &scheduleData,
	}
}

func (c *DPFMAPICaller) headerDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ScheduleLine) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderRead(input, log)
	if header == nil {
		return nil, nil, nil
	}
	header.HeaderIsDeleted = input.Orders.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
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
		(*items)[i].ItemIsDeleted = input.Orders.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*items)[i], "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot delete"
			return nil, nil, nil
		}
	}

	schedules := c.ScheduleLineRead(input, log)
	for i := range *schedules {
		(*schedules)[i].IsMarkedForDeletion = input.Orders.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*schedules)[i], "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Schedule Line Data cannot delete"
			return nil, nil, nil
		}
	}

	return header, items, schedules
}

func (c *DPFMAPICaller) itemDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ScheduleLine) {
	sessionID := input.RuntimeSessionID
	schedules := c.ScheduleLineRead(input, log)
	item := input.Orders.Item[0]
	for _, v := range *schedules {
		v.IsMarkedForDeletion = item.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": v, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Schedule Line Data cannot delete"
			return nil, nil
		}
	}

	items := make([]dpfm_api_output_formatter.Item, 0)
	for _, v := range input.Orders.Item {
		data := dpfm_api_output_formatter.Item{
			OrderID:            input.Orders.OrderID,
			OrderItem:          v.OrderItem,
			ItemDeliveryStatus: nil,
			ItemIsDeleted:      v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot delete"
			return nil, nil
		}
	}

	// itemがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Orders.Item[0].IsMarkedForDeletion {
		header := c.HeaderRead(input, log)
		header.HeaderIsDeleted = input.Orders.Item[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
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

	return &items, schedules
}

func (c *DPFMAPICaller) scheduleDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ScheduleLine {
	sessionID := input.RuntimeSessionID
	schedules := make([]dpfm_api_output_formatter.ScheduleLine, 0)
	for _, item := range input.Orders.Item {
		for _, schedule := range item.ItemSchedulingLine {
			data := dpfm_api_output_formatter.ScheduleLine{
				OrderID:             input.Orders.OrderID,
				OrderItem:           item.OrderItem,
				ScheduleLine:        schedule.ScheduleLine,
				IsMarkedForDeletion: schedule.IsMarkedForDeletion,
			}
			res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
			if err != nil {
				err = xerrors.Errorf("rmq error: %w", err)
				log.Error("%+v", err)
				return nil
			}
			res.Success()
			if !checkResult(res) {
				output.SQLUpdateResult = getBoolPtr(false)
				output.SQLUpdateError = "Order Item Schedule Line Data cannot delete"
				return nil
			}
			schedules = append(schedules, data)
		}
	}
	return &schedules
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
