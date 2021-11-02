import pandas as pd
import numpy as np
from operator import itemgetter
import copy

# ====================================================================================
#             The following code was copied from RSOXS profile collection
#             Find a way to import the code in order to avoid duplication

from databroker.v0 import Broker

catalog_name = "rsoxs"
try:
    db0 = Broker.named(catalog_name)
except Exception as ex:
    db0 = None
    print(f"Databroker catalog '{catalog_name}' can not be accessed: {ex}")


def giveme_inputs(*args, **kwargs):
    return args, kwargs


def string_to_inputs(string):
    return eval("giveme_inputs(" + string + ")")


def add_acq(sample_dict, plan_name="full_carbon_scan", arguments="", config="WAXS", priority=50):
    sample_dict["acquisitions"].append(
        {
            "plan_name": plan_name,
            "args": string_to_inputs(arguments)[0],
            "kwargs": string_to_inputs(arguments)[1],
            "configuration": config,
            "priority": priority,
        }
    )
    return sample_dict


def load_samplesxls(filename):
    df = pd.read_excel(
        filename,
        na_values="",
        engine="openpyxl",
        keep_default_na=True,
        converters={"sample_date": str},
        sheet_name="Samples",
        verbose=True,
    )
    df.replace(np.nan, "", regex=True, inplace=True)
    samplenew = df.to_dict(orient="records")
    if not isinstance(samplenew, list):
        samplenew = [samplenew]
    if "acquisitions" not in samplenew[0].keys():
        for samp in samplenew:
            samp["acquisitions"] = []
        acqsdf = pd.read_excel(
            filename,
            na_values="",
            engine="openpyxl",
            keep_default_na=True,
            sheet_name="Acquisitions",
            usecols="A:E",
            verbose=True,
        )
        acqs = acqsdf.to_dict(orient="records")
        if not isinstance(acqs, list):
            acqs = [acqs]
        for acq in acqs:
            if np.isnan(acq["priority"]):
                break
            samp = next(dict for dict in samplenew if dict["sample_id"] == acq["sample_id"])
            add_acq(
                samp,
                acq["plan_name"],
                acq["arguments"],
                acq["configuration"],
                acq["priority"],
            )
    else:
        for i, sam in enumerate(samplenew):
            samplenew[i]["acquisitions"] = eval(sam["acquisitions"])
    for i, sam in enumerate(samplenew):
        samplenew[i]["location"] = eval(sam["location"])
        samplenew[i]["bar_loc"] = eval(sam["bar_loc"])
        if "acq_history" in sam.keys():
            samplenew[i]["acq_history"] = eval(sam["acq_history"])
        else:
            samplenew[i]["acq_history"] = []
        samplenew[i]["bar_loc"]["spot"] = sam["bar_spot"]
        for key in [key for key, value in sam.items() if "named" in key.lower()]:
            del samplenew[i][key]
    return samplenew


def avg_scan_time(plan_name, nscans=50, new_scan_duration=600):
    if plan_name == "normal_incidence_rotate_pol_nexafs":
        multiple = 6
        plan_name = "fly_Carbon_NEXAFS"
    elif (plan_name == "fixed_pol_rotate_sample_nexafs") or (plan_name == "fixed_sample_rotate_pol_nexafs"):
        multiple = 5
        plan_name = "fly_Carbon_NEXAFS"
    else:
        multiple = 1
    scans = db0(plan_name=plan_name)
    durations = np.array([])
    for i, sc in enumerate(scans):
        if "exit_status" in sc.stop.keys():
            if sc.stop["exit_status"] == "success":
                durations = np.append(durations, sc.stop["time"] - sc.start["time"])
            if i > nscans:
                break
    if len(durations) > 0:
        return np.mean(durations) * multiple
    else:
        # we have never run a scan of this type before (?!?) - assume it takes some default value (10 min)
        scans = db0(master_plan=plan_name)
        durations = np.array([])
        for i, sc in enumerate(scans):
            if "exit_status" in sc.stop.keys():
                if sc.stop["exit_status"] == "success":
                    durations = np.append(durations, sc.stop["time"] - sc.start["time"])
                if i > nscans:
                    break
        if len(durations) > 0:
            return np.mean(durations) * multiple
        else:
            return new_scan_duration


def load_xlsx_to_plan_list(filename, sort_by=["sample_num"], rev=[False], retract_when_done=False):
    """
    run all sample dictionaries stored in the list bar
    @param bar: a list of sample dictionaries
    @param sort_by: list of strings determining the sorting of scans
                    strings include project, configuration, sample_id, plan, plan_args, spriority, apriority
                    within which all of one acquisition, etc
    @param dryrun: Print out the list of plans instead of actually doing anything - safe to do during setup
    @param rev: list the same length of sort_by, or booleans, wetierh to reverse that sort
    @param delete_as_complete: remove the acquisitions from the bar as we go, so we can automatically start back up
    @param retract_when_done: go to throughstation mode at the end of all runs.
    @param save_as_complete: if a valid path, will save the running bar to this position in case of failure
    @return:
    """
    bar = load_samplesxls(filename)
    list_out = []
    for samp_num, s in enumerate(bar):
        sample = s
        sample_id = s["sample_id"]
        sample_project = s["project_name"]
        for acq_num, a in enumerate(s["acquisitions"]):
            if "priority" not in a.keys():
                a["priority"] = 50
            list_out.append(
                [
                    sample_id,  # 0  X
                    sample_project,  # 1  X
                    a["configuration"],  # 2  X
                    a["plan_name"],  # 3
                    # If databroker can not be accessed, then assume the time is 0
                    avg_scan_time(a["plan_name"], 50) if db0 else 0,  # 4 calculated plan time
                    copy.deepcopy(sample),  # 5 full sample dict
                    a,  # 6 full acquisition dict
                    samp_num,  # 7 sample index
                    acq_num,  # 8 acq index
                    a["args"],  # 9  X
                    s["density"],  # 10
                    s["proposal_id"],  # 11 X
                    s["sample_priority"],  # 12 X
                    a["priority"],
                ]
            )  # 13 X
    switcher = {
        "sample_id": 0,
        "project": 1,
        "config": 2,
        "plan": 3,
        "plan_args": 9,
        "proposal": 11,
        "spriority": 12,
        "apriority": 13,
        "sample_num": 7,
    }
    # add anything to the above list, and make a key in the above dictionary,
    # using that element to sort by something else
    try:
        sort_by.reverse()
        rev.reverse()
    except AttributeError:
        if isinstance(sort_by, str):
            sort_by = [sort_by]
            rev = [rev]
        else:
            print(
                "sort_by needs to be a list of strings\n"
                "such as project, configuration, sample_id, plan, plan_args, spriority, apriority"
            )
            return
    try:
        for k, r in zip(sort_by, rev):
            list_out = sorted(list_out, key=itemgetter(switcher[k]), reverse=r)
    except KeyError:
        print(
            "sort_by needs to be a list of strings\n"
            "such as project, configuration, sample_id, plan, plan_args, spriority, apriority"
        )
        return
    plan_list = []
    for step in list_out:
        kwargs = step[6]["kwargs"]
        sample_md = step[5]
        # del sample_md['acquisitions']
        # if hasattr(rsoxs_queue_plans, step[3]):
        if True:
            kwargs.update(
                {
                    "configuration": step[2],
                    "sample_md": sample_md,
                    "acquisition_plan_name": step[3],
                }
            )
            plan = {"name": "run_queue_plan", "kwargs": kwargs, "item_type": "plan"}
            plan_list.append(plan)
        else:
            print(f"Invalid acquisition:{step[3]}, skipping")
    if retract_when_done:
        plan_list.append({"name": "all_out", "item_type": "plan"})
    return plan_list


# =======================================================================================

# NOTE: don't change the name of the function!!!
def spreadsheet_to_plan_list(*, spreadsheet_file, file_name, data_type, user, **kwargs):
    """
    Convert spreadsheet into a list of plans that could be added to the queue.

    Parameters
    ----------
    spreadsheet_file: file
        Readable file object.
    file_name: str
        The name of uploaded spreadsheet file.
    data_type: str, None
        Data type. Currently supported data types: ``wheel_xafs``.
    user: str
        User name: may be used as part of plan parameters.
    **kwargs: dict
        Passed to spreadsheet processing function.

    Returns
    -------
    plan_list : list(dict)
        Dictionary representing a list of plans extracted from the spreadsheet.
    """
    import os

    supported_extensions = ".xlsx"
    ext = os.path.splitext(file_name)[1]
    if ext not in supported_extensions:
        raise ValueError(
            f"Unsupported spreadsheet file '{file_name}' (extension '{ext}'). "
            f"Only extensions {supported_extensions} are supported"
        )

    # Some parameters must be lists
    if "rev" in kwargs:
        kwargs["rev"] = [kwargs["rev"]]
    if "sort_by" in kwargs:
        kwargs["sort_by"] = [kwargs["sort_by"]]

    # Data type is ignored, because it is assumed that there is only one type of spreadsheets.
    return load_xlsx_to_plan_list(filename=spreadsheet_file, **kwargs)
