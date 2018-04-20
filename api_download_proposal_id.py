""" This code is intended to provide a command-line tool for submitting a MAST
API query with a user-provided Proposal ID.  The script will then display some
statistics on the query results, and through prompts, allow the user to
download files associated with the most recent Observation ID.

The primary use case for this is JWST DMS rehearsals, where users wish to
test API functionality on incoming test data in real time.  The global
TELESCOPE variable can be set to other telescopes for script testing though.

..usage::  Simply launch the script in a terminal with a 'python
           api_download_proposal_id.py' command and terminal prompts for user
           input should guide you through the remaining steps.

..module::  start_proposal_id_check
..synopsis::  Get a proposal ID from user input and execute an initial count
              query.  Launch the following full query and download steps as
              the user chooses.

..module::  proposal_id_query
..synopsis::  Construct a mashup request to submit to mastQuery.  This
              includes filters for telescope and proposal ID.  Can handle both
              simple count queries and full results queries.

..module::  mastQuery
..synopsis::  Construct and submit a MAST API query.

..module::  download_latest_obs
..synopsis::  Identify the most recent observation in a set of MAST query
              results and find all files associated with this observation.
              Download these files if prompted by the user.

..module::  launch_mast_download
..synopsis::  Construct Threads to submit download requests for every file in
              a MAST Products query.

..module::  download_single_file
..synopsis::  Establish a MAST server connection and submit a data product
              download request for a single row of a MAST query results list.
"""


from astropy.time import Time
import json
import os
import re
import sys
from threading import Thread
from timeit import default_timer as timer

try: # Python 3.x
    from urllib.parse import quote as urlencode
    from urllib.request import urlretrieve
except ImportError:  # Python 2.x
    from urllib import pathname2url as urlencode
    from urllib import urlretrieve

try: # Python 3.x
    import http.client as httplib
except ImportError:  # Python 2.x
    import httplib

_TELESCOPE = "JWST"
_MAST_SERVER = "masttest.stsci.edu"

#--------------------

def start_proposal_id_check():
    """ Get the user to input an initial proposal ID and run an initial count
    query.  Retry until the count is in an acceptable range, then launch
    deeper inspection and download if the user chooses.
    """

    # Get a proposal ID to check
    propid = input("Enter a Proposal ID to check: ")

    # Get the initial file count based on the provided propid
    count = proposal_id_query(propid)

    # Try again if the file count is too big or zero
    while count > 50000 or count == 0:
        if count == 0:
            err = "No files found!  Try a different proposal: "
        elif count > 50000:
            err = "Too many results returned!  Try a different proposal: "
        else:
            break
        propid = input(err)
        count = proposal_id_query(propid)

    # Prompt the user to continue
    response = input("Found {0} observations for {1}.  Inspect the latest "
                     "observation? [y/n] ".format(count, propid))

    # If prompted, submit the full filtered query and pass that along to
    # download_latest_obs
    if response.lower() == 'y':
        query_dictionary = proposal_id_query(propid, count=False)
        download_latest_obs(query_dictionary)
    else:
        start_proposal_id_check()

#--------------------

def proposal_id_query(proposal_id, count=True):
    """ Construct a filtered proposal mashup request to send to the mastQuery
    module.  Return either the results of the query or just the results count.

    :param proposal_id:  The proposal ID to search for.
    :type proposal_id:  str

    :param count:  Flag to designate whether a full query is submitted, or
                   just the count results.  Also affects the returned
                   product.  Defaults to True.
    :type count:  bool
    """

    if proposal_id.lower() == "q":
        quit()
    try:
        int(proposal_id)
    except ValueError:
        print("Proposal ID must be an integer!")
        start_proposal_id_check()

    # Determine whether this is a full query or just a count
    if count:
        columns = "COUNT_BIG(*)"    # This will only get a count of the results
    else:
        columns = "*"

    # Construct the mashup request
    service = "Mast.Caom.Filtered"
    filters = [{"paramName":"obs_collection", "values":[_TELESCOPE]},
               {"paramName":"proposal_id", "values":[proposal_id]}
              ]
    mashupRequest = {"service":service,
                     "format":"json",
                     "params":{"columns":columns,
                               "filters":filters
                              }
                    }

    # Send the query
    headers, outString = mastQuery(mashupRequest)
    queryResults = json.loads(outString)

    # Return either the full query results or just the results count
    if count:
        data = queryResults['data']
        count = data[0]['Column1']
        return count
    else:
        return queryResults

#--------------------

def mastQuery(request):
    """Perform a MAST query.

        Parameters
        ----------
        request (dictionary): The MAST request json object

        Returns head,content where head is the response HTTP headers, and
        content is the returned data"""

    server = _MAST_SERVER

    # Grab Python Version
    version = ".".join(map(str, sys.version_info[:3]))

    # Create Http Header Variables
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain",
               "User-agent":"python-requests/"+version}

    # Encoding the request as a json string
    requestString = json.dumps(request)
    requestString = urlencode(requestString)

    # opening the https connection
    conn = httplib.HTTPSConnection(server)

    # Making the query
    conn.request("POST", "/api/v0/invoke", "request="+requestString, headers)

    # Getting the response
    resp = conn.getresponse()
    head = resp.getheaders()
    content = resp.read().decode('utf-8')

    # Close the https connection
    conn.close()

    return head, content

#--------------------

def download_latest_obs(query_dictionary):
    """ Identify the latest observation present in a set of query results
    (based on start time).  Retrieve some stats on this observation and prompt
    the user before downloading associated files.

    :param query_dictionary:  The dictionary of data products from a MAST
                              filtered query.
    :type query_dictionary:  dict
    """

    data_entries = query_dictionary['data']

    # Find the latest t_min value and the data entry associated with it
    times = [float(e['t_min']) for e in data_entries if e['t_min'] is not None]

    # If times is empty, probably hit a planned proposal
    if len(times) == 0:
        print("Data entries do not contain timing information!  (could be "
              "planned observations)")
        print("Starting over...")
        start_proposal_id_check()

    latest = times.index(max(times))
    latest_entry = data_entries[latest]
    obsid = str(latest_entry['obsid'])

    # Output some stats on the latest observation
    print("     LATEST OBSERVATION:")
    print("     obsid: {0}".format(latest_entry['obsid']))
    print("     proposal_id: {0}".format(latest_entry['proposal_id']))
    print("     PI: {0}".format(latest_entry['proposal_pi']))
    print("     target_name: {0}".format(latest_entry['target_name']))
    mjd = latest_entry['t_min']
    t = Time(mjd, format='mjd')
    print("     t_min: {0}".format(t.isot))

    # Construct and submit the Products request for files associated with the
    # latest obsid
    productRequest = {"service" : "Mast.Caom.Products",
                      "params" : {"obsid" : obsid},
                      "format" : "json",
                      "pagesize" : 100,
                      "page" : 1}
    headers, obsProductsString = mastQuery(productRequest)
    obsProducts = json.loads(obsProductsString)

    # Compile some stats on the associated files found
    files_found = len(obsProducts['data'])
    download_size = sum(f['size'] for f in obsProducts['data'])

    # Make the download size readable
    download_size = int(download_size / 1000)
    n = 0
    while len(str(download_size)) > 3:
        download_size = int(download_size / 1000)
        n += 1

    if n == 0:
        bytes = 'kb'
    elif n == 1:
        bytes = 'MB'
    elif n == 2:
        bytes = 'GB'
    else:
        print("Total file size too large!")
        start_proposal_id_check()

    size = "~{0} {1}".format(download_size, bytes)
    print("Found {0} files associated with {1} ({2} total file size)".format(
                                                                files_found,
                                                                obsid,
                                                                size))
    dl = input("Download these files? [y/n] ")

    # If the user chooses to download, send the data products along to
    # launch_mast_download
    if dl.lower() == 'y':
        start = timer()
        n = launch_mast_download(obsProducts['data'])
        duration = timer() - start
        print("...downloaded {0} of {1} files in {2} sec".format(n,
                                                                 files_found,
                                                                 duration))

    restart = input("Check another Proposal ID? [y/n] ")
    if restart.lower() == "y":
        start_proposal_id_check()
    else:
        quit()

#--------------------

def launch_mast_download(scienceProducts):
    """ Download files from a previous MAST Products query using Threads.

    :param scienceProducts:  A list of data products to be downloaded.
    :type scienceProducts:  list
    """

    # Create the directory (if needed) before launching threads
    firstrow = scienceProducts[0]
    outdir = os.path.join("mastFiles",
                          firstrow['obs_collection'],
                          firstrow['obs_id'],
                         )
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Iterate over each row of the scienceProducts list and construct a Thread
    threads = [Thread(target=download_single_file, args=(row,))
               for row in scienceProducts]

    # Start all the threads
    [th.start() for th in threads]
    print("...all download threads launched...")

    # Wait for threads to complete
    [th.join() for th in threads]

    # Get a file count of the target directory and return it for comparison
    # with the number of requested files.
    numfiles = len(os.listdir(outdir))
    return numfiles

#--------------------

def download_single_file(filerow):
    """ Download a single file from MAST.

    :param filerow:  One entry from a previous MAST query list of results.
    :type filerow:  dict
    """

    server = _MAST_SERVER
    conn = httplib.HTTPSConnection(server)

    # Make output file path
    outfile = os.path.join("mastFiles",
                           filerow['obs_collection'],
                           filerow['obs_id'],
                           filerow['productFilename']
                          )

    # Download the data
    uri = filerow['dataURI']
    conn.request("GET", "/api/v0/download/file?uri="+uri)
    resp = conn.getresponse()
    fileContent = resp.read()

    # Save to file
    with open(outfile,'wb') as FLE:
        FLE.write(fileContent)

    # Check that the file saved correctly
    if os.path.isfile(outfile):
        print("COMPLETE: ", outfile)
    else:
        print("ERROR: " + outfile + " failed to download.")

    conn.close()

#--------------------

if __name__ == "__main__":

    print("(use 'q' to quit)")
    print("CURRENT SETTINGS:")
    print("    telescope: {0}".format(_TELESCOPE))
    print("    server: {0}".format(_MAST_SERVER))
    start_proposal_id_check()
