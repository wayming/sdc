import grpc
from concurrent import futures
import time
import scrape_pb2
import scrape_pb2_grpc
import json
import traceback
from parsel import Selector
import logging

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Extract data from finanical pages
#
# Input:
#
# <table id="main-table" data-test="financials">
#   <thead>
#    // Fiscal Quarter
#     <tr>
#       // Key
#       <th></th>
#       // Current (optional)
#       <th></th>
#       // Data Columns
#       <th></th>
#       ...
#       // "Upgrade" column
#       <th></th>
#     </tr>
#    // Period Ending
#     <tr>
#       // Key
#       <th></th>
#       // Current (optional)
#       <th>
#           // Hidden span
#           <span></span>
#           <span></span>
#       </th>
#       ...
#       // "Upgrade" column
#       <th></th>
#     </tr>
#   </thead>
#   <tbody>
#     <tr>
#       // Key
#       <td></td>
#       // Current
#       <td></td>
#       // Data Columns
#       <td></td>
#       ...
#       // "Upgrade" column
#       <td></td>
#     </tr>
#     ...
#   </tbody>
# </table>
#
# Output:
#
#   [
#     {
#       "Period Ending": "Sep '24",
#       "Revenue": "100",
#       ...
#     },
#     {
#       "Period Ending": "Jun '24",
#       "Revenue": "95",
#       ...
#     },
#     ... 
#   ]
#
def handle_finanical_table(request):
    # Logic for handling financial table
    selector = Selector(request.html_text)
    logging.debug("Begin handle_finanical_table")
    try:
        theader = selector.xpath('//*[@id="main-table"]/thead/tr[1]/th')
        if len(theader) <= 0:
            return scrape_pb2.ERROR_PARSER, {"message": "No table header found"}

        periodEndings = selector.xpath('//*[@id="main-table"]/thead/tr[2]/th')

        trs = selector.xpath('//*[@id="main-table"]/tbody/tr')
        if len(trs) <= 0:
            return scrape_pb2.ERROR_PARSER, {"message": "No table body found"}

        # First column is the key of the header
        headerKey = periodEndings[0].xpath('.//text()').get()
        logging.debug("headerKey=%s", headerKey)

        # Populate the effective column
        # Exclude the key, current(if any) and upgrade column
        length = len(theader)
        hasTarget = False
        if theader[1].xpath('.//text()').get().upper() == 'CURRENT':
            hasTarget = True
            theader = theader[2:length-1]
            periodEndings = periodEndings[2:length-1]
        else:
            theader = theader[1:length-1]
            periodEndings = periodEndings[1:length-1]
            
        numOfEffectiveColumns = len(periodEndings)
              
        for tr in trs:
            tds = tr.xpath('.//td')
            # Some pages has unaligned columns, see https://stockanalysis.com/stocks/blne/financials/ratios/?p=quarterly
            # Set the effective columns to match the columns of the row with the fewest columns.
            rowEffectiveColumns = len(tds) - 2
            if hasTarget:
                rowEffectiveColumns = rowEffectiveColumns - 1
                
            if rowEffectiveColumns < numOfEffectiveColumns:
                numOfEffectiveColumns = rowEffectiveColumns

        logging.debug("Total number of effective columns is %d", numOfEffectiveColumns)

        # Populate number of columns to return
        results = []

        # Iterate over the extracted <th> elements and print their text
        for th in periodEndings:
            text = th.xpath('.//text()').get()  # Extract text content from the hidden <span> of the <th>
            results.append({ headerKey: text})

        logging.debug("results header: %s\n", json.dumps(results, indent=4))


        for tr in trs:
            tds = tr.xpath('.//td')
            
            # First column is the key of the row
            rowKey = tds[0].xpath('.//div//text()').get()
            if rowKey is None:
                rowKey = tds[0].xpath('.//a//text()').get()
            logging.debug("rowKey=%s", rowKey)
            
            # Remove the key column and target column if any
            # Remove the trailing upgrade column
            if hasTarget:
                tds = tds[2:len(tds)-1]
            else:
                tds = tds[1:len(tds)-1]
            
            if len(tds) < numOfEffectiveColumns:
                error = f"Expecting {numOfEffectiveColumns} effective columns, however got {len(tds)} columns from the row."
                logging.debug(error)
                return scrape_pb2.ERROR_PARSER, {"message": error}
            
            # Remaining columns are data for each fiscal period
            fiscalPeriodsIdx = 0
            for td in tds:
                results[fiscalPeriodsIdx][rowKey] = td.xpath('.//text()').get()
                fiscalPeriodsIdx = fiscalPeriodsIdx + 1

        logging.debug("Done handle_finanical_table")

        return scrape_pb2.OK, results
    except Exception as e:
            # Capture the exception and its traceback as a string
            error_trace = traceback.format_exception(type(e), e, e.__traceback__)
            
            # Convert the traceback into a single string (it's a list of strings)
            error_trace_str = ''.join(error_trace)
            logging.debug("Done handle_finanical_table")
            return scrape_pb2.ERROR_INTERNAL, {"message": error_trace_str}


def handle_balance_sheet(request):
    # Logic for handling balance_sheet
    return scrape_pb2.OK, {"parsed_data": "Balance Sheet Data"}

def handle_cash_flow(request):
    # Logic for handling cash_flow
    return scrape_pb2.OK, {"parsed_data": "Cash Flow Data"}

def handle_unknown(request):
    # Logic for handling unknown page types
    return scrape_pb2.OK, {"error": "Unknown page type"}

# Create a dictionary to map page_type to handler function
PAGE_TYPE_HANDLERS = {
    "finanical_table": handle_finanical_table,
}

# Implemente gRPC service
class ScraperService(scrape_pb2_grpc.HtmlScraperServicer):
    def ProcessPage(self, request, context):
        page_type = request.page_type
        
        # Select the handler based on the page_type, default to handle_unknown if not found
        handler = PAGE_TYPE_HANDLERS.get(page_type, handle_unknown)
        
        # Call the handler function and get the JSON data
        status, json_data = handler(request)
        
        # Return the parsed data as JSON
        return scrape_pb2.Response(status=status, json_data=json.dumps(json_data))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    scrape_pb2_grpc.add_HtmlScraperServicer_to_server(ScraperService(), server)
    server.add_insecure_port('[::]:50051')
    print("Starting server on port 50051...")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
