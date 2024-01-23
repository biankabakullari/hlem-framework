from dataclasses import dataclass, field
import steps
import preprocess
from preprocess import ProjectLogArgs
import component
from frames import framing, sorted_ids_by_ts
from hle_generation.eval_fw import eval_hlf, Aspect
from hle_generation.hle_generation_fw import TrafficOfInterest, generate_hle, filter_hla
from hle_connection.linkage import entity_pair_link, uniform_spread_link
from hle_connection.correlation_by_linkage import hle_graph_weighted, cascade_id
import hl_log.hl_log as hl_log
from hl_log.hl_log import HlLogArgs
from datetime import timezone
import logging
from typing import List, Literal, Union
from frames import Frame


@dataclass
class HlemArgs:
    """
    frame: determines the window size either by number (time scope will be equally distributed into number of windows)
    or by time unit ('minutes', 'hours', 'days', or 'weeks'), default is 'days'
    traffic_of_interest: the type of congestion that should be captured, can be 'low', 'high', or 'low and high',
    default is 'high'
    aspects: a list of the aspects that should be captures, also referred to as the high-level feature types
    p: the extremity threshold, depending on traffic_of_interest, if set to e.g., 0.8, then the 20% highest captured
    aspect values and/or 20% lowest captured aspect values will generate high-level events, default is 0.8
    link_thresh: the threshold used for connecting two high-level events of consecutive windows, default is 0.5, any two
    hle with link > link_thresh will be connected into a cascade
    spread_link: if True, the normalized link values will be reassigned so that they spread uniformly between 0 and 1,
    default is False
    hla_freq_thresh: the threshold to keep only most frequent high-level activities, either a float (e.g., 0.8 to keep
    20% most frequent hla, 1 to keep 100%) or a number > 1 (e.g., 10 for those hla whose frequency is in the top 10 ).
    After having computed all hle and cascades BEFORE filtering, the hle will be projected onto the most frequent hla
    only, the rest will disappear from output, default is 10
    only_entity: if True, the names of the high-level activities will only refer to the underlying entity (activity,
    resource, or segment), e.g., will not be 'exes-a', but only 'a'. This should only be used when the
    traffic_of_interest is 'low' or 'high' and not both.
    aspect_based: if True, the threshold to generate high-level events like e.g., 'exec-a' will be based on all 'exec'
    values measured for all activities of interest, and not only 'a', default is False. Should be set to True only if
    the selected activities have similar execution counts across the log
    seg_method: the way to determine when an event pair constitutes a step in the process, default is 'directly-follows'
    flatten: if True, hle within the same window of the same cascade get slightly different ts. Otherwise, hle of same
    window of same cascade will have identical ts (the window's left border). Default is False.
    """
    frame: Frame = 'days'
    traffic_of_interest: TrafficOfInterest = 'high'
    aspects: List[Aspect] = field(default_factory=lambda: ['enter', 'exit', 'wait', 'exec'])
    p: float = 0.8
    link_thresh: float = 0.5
    spread_link: bool = False
    hla_freq_thresh: Union[float, int, str] = 0.8
    only_entity: bool = False
    aspect_based: bool = False
    seg_method: Literal['df', 'mf'] = 'df'
    flatten: bool = False

    def __post_init__(self):
        self.validate_p()
        self.validate_traffic()
        self.validate_link_thresh()
        self.validate_hla_freq_thresh()

    def validate_p(self):
        if not (0 < self.p < 1):
            raise ValueError('Extremity threshold p must be in range 0.5 < p < 1')

    def validate_traffic(self):
        if self.traffic_of_interest not in ['low', 'high', 'low and high']:
            raise ValueError('The traffic of interest must be either low, high, or low and high.')

    def validate_link_thresh(self):
        if not (0 < self.link_thresh < 1):
            raise ValueError('The link threshold must be in range 0 < link_thresh < 1')

    def validate_hla_freq_thresh(self):
        if isinstance(self.hla_freq_thresh, str) and self.hla_freq_thresh != "all":
            raise ValueError("hla_freq_thresh must be a float, an int, or the string 'all'")


def fix_res_config(res_selection, aspects):
    """
    :param res_selection: Either 'all', or a list of resource names (strings), or an empty list, or a float between 0
    and 1
    :param aspects: Either 'all', or a list of aspects (e.g., 'enter', 'exec', 'busy'), or an empty list
    :return:
    res_info: bool, set to True if at least one of the aspects is resource related, otherwise False
    res_selection: returns the chosen res_selection
    ValueError raised if resource related aspects are chosen, but the res_selection is empty
    """

    # if no resource-based aspect chosen, then do not analyze resources
    if 'do' not in aspects and 'todo' not in aspects and 'busy' not in aspects:
        res_info = False
        res_selection = []

    # at least one resource-based aspect is chosen
    else:
        # if at least one specific resource is chosen for analysis or the choice is made based on coverage (float)
        if len(res_selection) or (isinstance(res_selection, float) and 0 < res_selection < 1):
            # at least one resource chosen, set res_info to True
            res_info = True
        # res_selection is []
        else:
            # no resource is chosen, raise error
            raise ValueError('If you want to analyze resource aspects, you must select at least one resource or set '
                             'res_selection to a positive number between 0 and 1 or to "all".')

    return res_info, res_selection


def transform_log_to_hl_log(log, act_selection, seg_selection, res_selection, args: HlemArgs,
                            project_args: ProjectLogArgs, export=True):
    """
    :param log: the input event log in xes format
    :param act_selection: the set of activities chosen for analysis, can be 'all', a list of activities, or a float
    between 0 and 1
    :param seg_selection: the set of segments chosen for analysis, can be 'all', a list of activity pairs, or a float
    between 0 and 1
    :param res_selection: the set of resources chosen for analysis, can be 'all', a list of resources, or a float
    between 0 and 1
    :param args: HlemArgs object with the configuration to turn a low-level log into a high-level log
    :param project_args: ProjectLog args with activity_coverage and segment_coverage used for cutting out event data
    before analysis
    :param export: bool, if True then the high-level log is exported into the current directory as .xes and .csv file,
    default is True
    :return:
    """

    res_info, res_selection = fix_res_config(res_selection, args.aspects)

    logging.info('Gathering information on steps, activities, resources, and segments.')
    # first: create event dictionary, event_pairs, trigger and release dicts, components
    event_dict = preprocess.create_event_dict(log, res_info)
    steps_list, trigger, release = steps.trigger_release_dicts(log, args.seg_method)

    # the following will cut out event data based on either activity or segment frequency,
    # avoid using unless necessary (e.g., memory issues with huge logs)
    event_dict, steps_list, trigger, release = preprocess.event_dict_projection(event_dict, steps_list, trigger,
                                                                                release, project_args.activity_coverage,
                                                                                project_args.segment_coverage)
    all_A, all_S, all_R = component.get_entities(event_dict, steps_list, res_info)
    A_focus, S_focus, R_focus = component.get_entities_for_analysis(event_dict, steps_list, res_info, act_selection,
                                                                    seg_selection, res_selection)

    component_types_dic = component.comp_type_dict(A_focus, S_focus, R_focus)

    print(f"Analyzed activities ({len(A_focus)}):")
    print(A_focus)
    print(f"Analyzed segments ({len(S_focus)}):")
    print(S_focus)
    print(f"Analyzed resources ({len(R_focus)}):")
    print(R_focus)

    logging.info('Computing windows, partitioning the events into windows.')
    ids_sorted = sorted_ids_by_ts(event_dict)
    window_to_ev_list, ev_to_window, window_to_borders = framing(event_dict, args.frame, ids_sorted)

    logging.info('Evaluating the chosen high-level activities across all time windows.')
    eval_hlf_complete = eval_hlf(event_dict, trigger, window_to_borders, ev_to_window, steps_list, res_info,
                                 A_focus, S_focus, R_focus, args.aspects)

    logging.info('Generating high-level events.')
    id_to_hle_all, window_to_id_to_hle, hla_freq = generate_hle(args.traffic_of_interest, eval_hlf_complete,
                                                                args.aspects, component_types_dic, args.p,
                                                                args.aspect_based)

    logging.info('Correlating high-level events into high-level cases.')
    # correlation in hlem using the link values: a value between 0 and 1 for each entity pair
    # note that the link values solely depend on the info in the log, the generated hle are not considered
    link = entity_pair_link(event_dict, steps_list, all_A, all_S, all_R, trigger, release, res_info)
    if args.spread_link:
        link = uniform_spread_link(link)
    G = hle_graph_weighted(window_to_id_to_hle, link, args.link_thresh)
    cascade_dict = cascade_id(G)

    logging.info('Projecting on frequent high-level activities')
    hla_list_filtered = filter_hla(hla_freq, args.hla_freq_thresh)

    tz_info = timezone.utc
    logging.info('Generating high-level log and dataframe')

    hl_log_args = HlLogArgs(args.only_entity, args.traffic_of_interest)
    hl_log_xes, hl_log_df = hl_log.generate_hl_xes_and_df(window_to_borders, window_to_id_to_hle, cascade_dict, tz_info,
                                                          hla_list_filtered, hl_log_args, args.flatten, args.frame,
                                                          export)

    return hl_log_xes, hl_log_df
