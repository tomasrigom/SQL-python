#Retrieval of SQL TAP queries

import pyvo
import os
import shutil

def fetch_results_of_complete_jobs(tap_service, urls_filename, overwrite = True, data_format = 'csv'):
    '''Fetch the results of complete jobs and return the number of unfinished ones

    Parameters:
    -----------
    tap_service: pyvo.dal.tap.TAPService
        The TAP service to which the query will be submitted. Requires the package pyvo

    urls_filename: str
        The filename of the file holding the urls of the jobs

    overwrite: str
        Determines whether the url file is overwritten with the unfinished jobs or if another one is created

    format: str
        Determines in what format the data is saved. The possibilities are:
            - csv : Comma Separated Values
            - xml : Extensible Markup Languaje
            - json: JavaScript Object Notation
            - parquet : Parquet
            - hdf5 : Hierarchical Data Format
            - feather : Feather
    '''

    running_job_names = []

    #
    # Recreate the job from url and session (token)
    #
 
    # read the url
    with open(urls_filename, 'r') as fd:
        job_urls = fd.readlines()

    #Create new url filename if we do not want to overwrite the previous one
        
    base_name, extension = os.path.splitext(urls_filename)
    urls_filename_unfinished = f"{base_name}_unfinished{extension}"

    #Open new url file
    with open(urls_filename_unfinished, 'w') as fd:

        # Query status and if COMPLETE fetch results
        for job_url in job_urls:

            # recreate the job
            job = pyvo.dal.AsyncTAPJob(job_url.rstrip('\n'), session=tap_service._session)

            #
            # Check the job status
            #
            print('JOB {name}: {status}'.format(name=job.job.runid , status=job.phase))

            # if still running --> exit
            if job.phase not in ("COMPLETED", "ERROR", "ABORTED"):
                running_job_names.append(job.job.runid)
                fd.write(job_url)
                continue

            #
            # Fetch the results
            #
            try:
                job.raise_if_error()
                print('fetching the results...')
                tap_results = job.fetch_result()
                print('writing results to disk...\n')
                table = tap_results.to_table()

                if data_format=='hdf5':
                    table.write('./' + str(job.job.runid) + '.h5', format='hdf5', overwrite=True)
                elif data_format=='xml':
                    table.write('./' + str(job.job.runid) + '.xml', format='votable', overwrite=True)
                elif data_format=='csv' or data_format=='json' or data_format=='parquet' or data_format=='feather':
                    table.write('./' + str(job.job.runid) + f'.{data_format}', format=data_format, overwrite=True)
                else:
                    print("Data format not recognised. Returning -1")
                    return -1
                #tap_results.votable.to_xml('./' + str(job.job.runid) + '.xml')

            except Exception as e:
                running_job_names.append(job.job.runid)
                fd.write(job_url)
                print(e)

        print('...DONE\n')

        # Output still running jobs
        try:
            assert(running_job_names == [])
        except AssertionError:
            print("The following jobs are still executing: {}".format(running_job_names))

    #If everything went smoothly and overwrite = True, overwrite the file
    if overwrite == True:
        shutil.move(urls_filename_unfinished, urls_filename)

    #Return the number of unfinished jobs
    return len(running_job_names)