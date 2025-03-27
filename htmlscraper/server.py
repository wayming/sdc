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
#       <td></td>
#       // Current
#       <td></td>
#       // Data Columns
#       <td></td>
#       ...
#       // "Upgrade" column
#       <td></td>
#     </tr>
#    // Period Ending
#     <tr>
#       <td></td>
#       ...
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
#       "Fiscal Quarter": 'Q3 2024',
#       "Revenue": "100",
#       ...
#     },
#     {
#       "Fiscal Quarter": 'Q2 2024',
#       "Revenue": "95",
#       ...
#     },
#     ... 
#   ]
#
def handle_finanical_table(request):
    # Logic for handling financial table
    selector = Selector(request.html_text)
    logging.debug("handle_finanical_table")
    try:
        theader = selector.xpath('//*[@id="main-table"]/thead/tr[1]/th')
        if len(theader) <= 0:
            return scrape_pb2.ERROR_PARSER, {"message": "No table header found"}

        trs = selector.xpath('//*[@id="main-table"]/tbody/tr')
        if len(trs) <= 0:
            return scrape_pb2.ERROR_PARSER, {"message": "No table body found"}

        # First column is the key of the header
        headerKey = theader[0].xpath('.//text()').get()
        logging.debug("headerKey=%s", headerKey)

        # Populate the effective column
        # Exclude the key, current and upgrade column
        numOfEffectiveColumns = len(theader)
        for tr in trs:
            tds = tr.xpath('.//td')
            # Some pages has unaligned columns, see https://stockanalysis.com/stocks/blne/financials/ratios/?p=quarterly
            # Set the effective columns to match the columns of the row with the fewest columns.
            if len(tds) < numOfEffectiveColumns:
                numOfEffectiveColumns = len(tds)
        numOfEffectiveColumns = numOfEffectiveColumns - 3 # Remove the key, current and upgrade column

        # Populate number of columns to return
        results = []
        theader = theader[1:numOfEffectiveColumns+1]

        # Iterate over the extracted <th> elements and print their text
        for th in theader:
            text = th.xpath('.//text()').get()  # Extract text content from <th>
            results.append({ headerKey: text})
            fiscalPeriodsIdx = fiscalPeriodsIdx + 1
        logging.debug("results header: %s\n", json.dumps(results, indent=4))


        for tr in trs:
            tds = tr.xpath('.//td')
            
            # First column is the key of the row
            rowKey = tds[0].xpath('.//div//text()').get()
            if rowKey is None:
                rowKey = tds[0].xpath('.//a//text()').get()
            logging.debug("rowKey=%s", rowKey)

            tds = tds[1:numOfEffectiveColumns+1]

            # Each row may have the same number of columns as the header or one column less
            if len(tds[1:]) < numOfEffectiveColumns:
                error = f"Expecting {numOfEffectiveColumns} effective columns, however got {len(tds[1:])} columns from the row."
                logging.debug(error)
                return scrape_pb2.ERROR_PARSER, {"message": error}
            
            # Remaining columns are data for each fiscal period
            fiscalPeriodsIdx = 0
            for td in tds:
                results[fiscalPeriodsIdx][rowKey] = td.xpath('.//text()').get()
                fiscalPeriodsIdx = fiscalPeriodsIdx + 1

        return scrape_pb2.OK, results
    except Exception as e:
            # Capture the exception and its traceback as a string
            error_trace = traceback.format_exception(type(e), e, e.__traceback__)
            
            # Convert the traceback into a single string (it's a list of strings)
            error_trace_str = ''.join(error_trace)
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
