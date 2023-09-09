package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-quotations-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-quotations-deletes-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Quotation = %d ", input.Quotations.Quotation)
	where = fmt.Sprintf("%s \n AND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	// where = fmt.Sprintf("%s \n AND ( header.HeaderDeliveryStatus, header.IsDdeleted, header.IsMarkedForDeletion ) = ( 'NP', false, false ) ", where)
	rows, err := c.db.Query(
		`SELECT 
		header.Quotation
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_quotations_header_data as header ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ItemsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Item {
	where := fmt.Sprintf("WHERE item.Quotation IS NOT NULL\nAND header.Quotation = %d", input.Quotations.Quotation)
	where = fmt.Sprintf("%s\nAND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
		item.Quotation, item.QuotationItem
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_quotations_item_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_quotations_header_data as header
		ON header.Quotation = item.Quotation ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToItem(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
