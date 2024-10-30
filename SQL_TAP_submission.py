#Submission of sql TAP queries

import pyvo

def submit_queries(tap_service, queries, lang='PostgreSQL', queue='5h', urls_filename='jobs_url.txt'):

    '''Submit a series of tap queries

    Parameters:
    --------
    tap_service: pyvo.dal.tap.TAPService
        The TAP service to which the query will be submitted. Getting this TAP service requires the package pyvo.

    queries: list(tuple)
        A list consisting of (query_name, query_string) pairs.

    lang: str, default: PostgreSQL
        The language in which the queries are written.

    queue: str, default: 5h
        The name of the queue to use. In this case it has a timeout of 5 hours. Check specifications of site to which we are queueing.

    urls_filename: str, default: jobs_url.txt
        The filename of the file holding the urls of the submitted jobs (for later retrieval).
    '''

    # list of failed jobs
    failed = []

    # open the file to store the jobs: for later retrieval
    with open(urls_filename, 'w') as fd:

        # submit all queries one after another
        for name, query in queries:

            print('     Submitting : {name}'.format(name=name))

            # Create the async job
            try:
                job = tap_service.submit_job(query, language=lang, runid=name, queue=queue)
            except Exception as e:
                print('ERROR could not create the job.')
                print(e)
                failed.append(name)
                continue        

            # Run the run
            try:
                job.run()
            except Exception as e:
                print('Error: could not run the job. Are you sure about: \n - validity of the SQL query?\n - valid language?\n - sufficient quotas?\n')
                print(e)
                failed.append(name)
                continue

            # Save the submitted jobs into a file
            fd.write(job.url + '\n')


        # Verify that all jobs have been submitted
        try:
            assert(failed == [])
            print("\nAll jobs were properly submitted.")
        except AssertionError:
            print("\nThe following jobs have failed: {jobs}".format(jobs=failed))

        fd.close()