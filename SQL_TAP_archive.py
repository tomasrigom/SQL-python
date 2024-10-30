import pyvo

#Archiving SQL TAP jobs based upon their status

def archive_queries(tap_service,tap_session,status="COMPLETED"):

    """
    Archives jobs of a specified status from a TAP service by deleting them.

    Parameters:
    tap_service : pyvo.dal.TAPService
        The TAP service instance used to interact with the TAP server. Requires the usage of pyvo package

    tap_session : requests.Session
        The session used for authenticated requests. Requires the usage of requests package

    status : str
        Status of the jobs to archive. 
        Options are "COMPLETED", "QUEUED", "PENDING", "ERROR", "EXECUTING", "ABORTED", "UNKNOWN", "HELD", "SUSPENDED", or "ALL" to archive jobs of all statuses. Defaults to "COMPLETED".
    """

    status_list = ["COMPLETED", "QUEUED", "PENDING", "ERROR", "EXECUTING", "ABORTED", "UNKNOWN", "HELD", "SUSPENDED"]

    # Check if 'ALL' is chosen or to include a single status
    if status == "ALL":
        statuses_to_archive = status_list
    elif status in status_list:
        statuses_to_archive = [status]
    else:
        print(f"Invalid status '{status}'. Please choose from {status_list} or 'ALL'.")
        return

    for current_status in statuses_to_archive:
        # obtain the list of completed job_descriptions
        try:
            job_descriptions = tap_service.get_job_list(phases=current_status)

        except Exception as e:
            print(f"Error fetching jobs with status {current_status}: {e}")
            continue

        # Archiving each of them
        for jobdescription in job_descriptions:
            try:
                # get the jobid
                job_id = jobdescription.jobid

                # recreate the url by hand
                job_url = tap_service.baseurl + '/async/' + job_id

                # recreate the job
                job = pyvo.dal.AsyncTAPJob(job_url, session=tap_session)

                print(f'Archiving job {job_id} with status "{current_status}": {job_url}')
                job.delete() # archive job


            except Exception as e:
                print(f"Error archiving job {job_id} with status '{current_status}': {e}")
                #Loop continues to the next element