#!/bin/bash

blocks=false
all=true
help=false

node_info_pbsnodes_property=""
awk_user_not_pgc='user != "husby" && user != "cporter" && user != "foga" && user != "klassen" && user != "noh1"'

script_args=("$@")
info_mode_args=()
status=0
for arg in "${script_args[@]}"; do
    if [ "$arg" == "runs" ] || [ "$arg" == "table" ] || [ "$arg" == "nodes" ]; then
        info_mode_args+=( "$arg" )
    elif [ "$arg" == "jobs" ] || [ "$arg" == "jobs-by-batch" ] || [ "$arg" == "jobs-by-node" ] || [ "$arg" == "jobs-by-user" ]; then
        info_mode_args+=( "$arg" )
    elif [ "$arg" == "blocks" ]; then
        blocks=true
    elif [ "$arg" == "all" ]; then
        all=true
    elif [ "$arg" == "help" ] || [ "$arg" == "-h" ] || [ "$arg" == "-help" ] || [ "$arg" == "--help" ]; then
        help=true
    else
        status=1
    fi
done
if (( status != 0 )) || [ "$help" == "true" ]; then
    echo "Arguments can include any of [<none>|runs|table|nodes|jobs|jobs-by-batch|jobs-by-node|jobs-by-user|blocks|all]"
    exit $status
fi
if (( ${#info_mode_args[@]} == 0 )); then
    info_mode_args+=( "<none>" )
fi

awk_user_not_pgc='user != "husby" && user != "cporter" && user != "foga" && user != "klassen" && user != "noh1"'
awk_user_filter=''
if [ "$all" == "false" ]; then
    awk_user_filter="${awk_user_filter}; if (${awk_user_not_pgc}) next;"
fi

jobinfo_list_descr() {
    jobinfo_mode="$1"
    if [ "$jobinfo_mode" == "jobs-by-batch" ]; then
        job_list_type="BATCH"
    elif [ "$jobinfo_mode" == "jobs-by-node" ]; then
        job_list_type="NODE"
    elif [ "$jobinfo_mode" == "jobs-by-user" ]; then
        job_list_type="USER"
    else
        job_list_type="STATUS"
    fi
    echo "$job_list_type"
}
jobinfo_block_descr() {
    blocks="$1"
    if [ "$blocks" == "true" ]; then
        job_count_type="Block"
    else
        job_count_type="Group"
    fi
    echo "$job_count_type"
}


time2sec() { 
    IFS=: read h m s <<<"${1%.*}"
    seconds=$((10#$s+10#$m*60+10#$h*3600))
    echo "$seconds"
}
sec2time() {
    ((h=${1}/3600))
    ((m=(${1}%3600)/60))
    ((s=${1}%60))
    printf "%02d:%02d:%02d\n" $h $m $s
}
parse_xml_value() {
    xml_tag="$1"
    while read xml_onelinestring; do
        echo "$xml_onelinestring" | grep -Eo "<${xml_tag}>(.*?)</${xml_tag}>" | sed -r "s|<${xml_tag}>(.*?)</${xml_tag}>|\1|"
    done
}


awk_uniq_all() {
    rows_blob="$1"
    delim="$2"
    col_nums="$3"
    jobinfo_mode="$4"
    # awk_cmd="!seen[${col_nums}]++"
    awk_cmd='BEGIN { pat_num=0 } { user=$2; jobabbrev=$4; sub(/[0-9]+$/, "", jobabbrev); sub(/_[0-9]+_$/, "", jobabbrev); pat='"${col_nums}"'; pat=sprintf("%s%s", jobabbrev, pat); '"${awk_user_filter}"' if ("'"${jobinfo_mode}"'" == "jobs-by-user") pat=user; if (!count[pat]++) { pat_num++; line[pat_num]=$0; line_pat[pat_num]=pat; } } END { for (i=1; i<=pat_num; i++) printf "%-125s %-7s\n", line[i], count[line_pat[i]] } '
    echo "$rows_blob" | awk -F" " "$awk_cmd"
}

awk_uniq_blocks() {
    rows_blob="$1"
    delim="$2"
    col_nums="$3"
    jobinfo_mode="$4"
    awk_cmd='BEGIN { last_pat=""; pat_num=0 } { user=$2; jobabbrev=$4; sub(/[0-9]+$/, "", jobabbrev); sub(/_[0-9]+_$/, "", jobabbrev); pat='"${col_nums}"'; pat=sprintf("%s%s", jobabbrev, pat); '"${awk_user_filter}"' if ("'"${jobinfo_mode}"'" == "jobs-by-user") pat=user; if (pat!=last_pat) { last_pat=pat; pat_num++; line[pat_num]=$0; count[pat_num]=1; } else { count[pat_num]++ } } END { for (i=1; i<=pat_num; i++) printf "%-125s %-7s\n", line[i], count[i] } '
    # mapfile -t uniq_first < <( echo "$rows_blob" | awk -F" " "$awk_cmd" )
    # mapfile -t uniq_last < <( echo "$rows_blob" | tac | awk -F" " "$awk_cmd" | tac )
    while IFS= read -r line; do uniq_first+=( "$line" ); done < <( echo "$rows_blob" | awk -F" " "$awk_cmd" )
    while IFS= read -r line; do uniq_last+=( "$line" ); done < <( echo "$rows_blob" | tac | awk -F" " "$awk_cmd" | tac )
    rows_uniq=()
    for i in "${!uniq_first[@]}"; do
        rows_uniq+=( "${uniq_first[$i]}" )
        if [ "${uniq_first[$i]}" != "${uniq_last[$i]}" ]; then
            rows_uniq+=( "${uniq_last[$i]}" )
        fi
    done
    printf '%s\n' "${rows_uniq[@]}"
}


print_job_info() {
    jobinfo_mode="$1"
    blocks="$2"
    job_list_type=$(jobinfo_list_descr "$jobinfo_mode")
    job_count_type=$(jobinfo_block_descr "$blocks")
qstat_header=$(cat <<EOF

${HOSTNAME}: [[ Job summary by ${job_list_type} ]]
                                                                                  Req'd    Req'd       Elap                   Jobs in
Job ID                  Username    Queue    Jobname          SessID  NDS   TSK   Memory   Time    S   Time      Node Name    ${job_count_type}
----------------------- ----------- -------- ---------------- ------ ----- ------ ------ --------- - ---------   ----------   -------
EOF
)
    if [ "$jobinfo_mode" == "jobs-by-batch" ]; then
        uniq_qstat_col_nums='$2$3'
    elif [ "$jobinfo_mode" == "jobs-by-node" ]; then
        uniq_qstat_col_nums='$2$3$12'
    elif [ "$jobinfo_mode" == "jobs-by-user" ]; then
        uniq_qstat_col_nums='$2'
    else
        uniq_qstat_col_nums='$2$3$10'
    fi
    qstat_info=$(qstat -l -n1)
    # qstat_header=$(echo "$qstat_info" | head -n 5)
    qstat_body=$(echo "$qstat_info" | tail -n +6 | grep -v " C " | cut -d"/" -f1)
    if [ "$blocks" == "true" ]; then
        qstat_uniq=$(awk_uniq_blocks "$qstat_body" ' ' "$uniq_qstat_col_nums" "$jobinfo_mode")
    else
        qstat_uniq=$(awk_uniq_all "$qstat_body" ' ' "$uniq_qstat_col_nums" "$jobinfo_mode")
    fi
    echo "$qstat_header"
    echo "$qstat_uniq"
}


jobids_to_runinfo() {
    while read jobids; do
        runinfo=$(qstat -lxf ${jobids} | parse_xml_value "submit_args" | rev | cut -d"/" -f3- | rev | awk '{print $1"/swift.out"}' | xargs -d'\n' grep -Ehs -m1 -- "/scratch/sciteam/.*\.sh,[0-9]+" | cut -d"," -f1 | awk -F"/" '{ reg=$8; jf_dname=$9; res=$10; if (jf_dname=="s2s_jobfiles") { prog="s2s" } else { prog="SETSM" }; printf "%-7s %-4s %s\n", prog, res, reg }')
        if [ -z "$runinfo" ]; then
            runinfo=$(printf "%-7s %-4s %s\n" "--" "--" "--")
        fi
        echo "$runinfo"
    done
}

jobids_to_himemfo() {
    while read jobids; do
        himemfo=$(qstat -lxf ${jobids} | parse_xml_value "nodes" | grep -Eo "[a-z]+himem")
        if [ -z "$himemfo" ]; then
            himemfo="--"
        fi
        printf "%-7s\n" $himemfo
    done
}

jobids_filter_node_property() {
    if [ -z "$node_info_pbsnodes_property" ]; then
        echo "$@"
        return
    fi
    for jobid in "$@"; do
        property=$(qstat -lxf ${jobid} | parse_xml_value "nodes" | grep "$node_info_pbsnodes_property")
        if [ -n "$property" ]; then
            echo -n "$jobid"
        fi
    done
    echo
}


print_job_info_bw() {
    jobinfo_mode="$1"
    blocks="$2"
    job_list_type=$(jobinfo_list_descr "$jobinfo_mode")
    job_count_type=$(jobinfo_block_descr "$blocks")
qstat_header_bw=$(cat <<EOF

${HOSTNAME}: [[ Job summary by ${job_list_type} ]]
                                                                                  Req'd    Req'd       Elap                   Jobs in
Job ID                  Username    Queue    Jobname          SessID  NDS   TSK   Memory   Time    S   Time      Node Name    ${job_count_type}   himem   Program Res  Regionname
----------------------- ----------- -------- ---------------- ------ ----- ------ ------ --------- - ---------   ----------   ------- ------- ------- ---- -----------------------
EOF
)
    jobinfo=$(print_job_info "$jobinfo_mode" "$blocks")
    # jobinfo_head=$(echo "$jobinfo" | head -n 5)
    jobinfo_body=$(echo "$jobinfo" | tail -n +6)
    jobinfo_arr=()
    himemfo_arr=()
    runinfo_arr=()
    while IFS= read -r line; do jobinfo_arr+=( "$line" ); done < <( echo "$jobinfo_body" )
    while IFS= read -r line; do himemfo_arr+=( "$line" ); done < <( echo "$jobinfo_body" | cut -d"." -f1 | jobids_to_himemfo )
    if [ "$all" == "false" ]; then
        while IFS= read -r line; do runinfo_arr+=( "$line" ); done < <( echo "$jobinfo_body" | cut -d"." -f1 | jobids_to_runinfo )
    fi
    echo "$qstat_header_bw"
    for i in $(seq 0 $(( ${#jobinfo_arr[@]} - 1 ))); do
        echo -n "${jobinfo_arr[i]}"
        if [ -n "${himemfo_arr[i]}" ]; then
            echo -n " ${himemfo_arr[i]}"
        fi
        if [ "$all" == "false" ] && [ -n "${runinfo_arr[i]}" ]; then
            echo -n " ${runinfo_arr[i]}"
        fi
        echo
    done
}


print_job_total() {
    echo
    showq | tail -n 3 | grep "Total Jobs"
}


node_info_header=$(cat <<EOF

NODE$(printf "%-11s" ${node_info_pbsnodes_property}) STATUS  MEMFREE COREUSE NUMJOBS USER(njobs)
--------------- ------- ------- ------- ------- -----------
EOF
)
print_node_info() {
    node_arr=()
    reserve_jobs=()
    if [ "$node_info_pbsnodes_property" == ":xehimem" ]; then
        while IFS= read -r line; do node_arr+=( "$line" ); done < <( pbsnodes all ${node_info_pbsnodes_property} -l | grep -v "offline" | cut -d" " -f1 | sort -u )
    else
        while IFS= read -r line; do node_arr+=( "$line" ); done < <( pbsnodes all ${node_info_pbsnodes_property} -l | cut -d" " -f1 | sort -u )
    fi
    while IFS= read -r line; do reserve_jobs+=( "$line" ); done < <( qstat -lr -n1 | grep " STDIN " )

    echo "$node_info_header"
    for node_name in "${node_arr[@]}"; do
        node_info=$(pbsnodes -x "$node_name")

        node_state=$(echo "$node_info" | parse_xml_value 'state')
        node_np=$(echo "$node_info" | parse_xml_value 'np')
        node_jobs=$(echo "$node_info" | parse_xml_value 'jobs')
        node_status=$(echo "$node_info" | parse_xml_value 'status')
        node_status_jobs=$(echo "$node_status" | grep -Po "jobs=.*?(,|$)" | cut -d"=" -f2 | cut -d"," -f1)
        node_status_jobs=$(jobids_filter_node_property ${node_status_jobs})

        if [ "$node_info_pbsnodes_property" == ":xehimem" ] && [ -z "$node_status_jobs" ]; then continue; fi

        node_np_in_use=$(echo "$node_jobs" | awk -F"/" '{print NF-1}')
        if [ "$node_np_in_use" == "-1" ]; then
            node_np_in_use=0
            node_njobs=0
        else
            IFS=',' read -r -a node_job_arr <<< "$node_jobs"
            node_njobs=$(printf '%s\n' "${node_job_arr[@]}" | cut -d"/" -f2 | sort -u | wc -l)
        fi
        node_coreuse="${node_np_in_use}/${node_np}"

        if [ -n "$node_status_jobs" ]; then
            awk_cmd='BEGIN { } { user=$3; '"${awk_user_filter}"' user_count[user]++; } END { for (user in user_count) { printf "%s(%s) ", user, user_count[user]; } printf "\n"; } '
            node_qstat_jobs=$(qstat -l ${node_status_jobs} | tail -n +3)
            user_njobs=$(echo "$node_qstat_jobs" | awk -F" " "$awk_cmd")
        else
            user_njobs=''
        fi

        if [ "$node_state" == "offline" ]; then node_state="N/A"; fi
        if [ "$node_state" == "job-exclusive" ]; then node_state="busy!"; fi
        if [ "$node_state" == "free" ]; then
            if ((node_np_in_use == 0)); then
                node_state="idle"
            else
                node_state="active"
            fi
        fi

        node_totmem=$(echo "$node_info" | grep -Eoi "totmem=[0-9]+kb" | sed -r "s|totmem=([0-9]+)kb|\1|")
        node_availmem=$(echo "$node_info" | grep -Eoi "availmem=[0-9]+kb" | sed -r "s|availmem=([0-9]+)kb|\1|")

        node_totmem_gb="$((node_totmem/1024/1024))"
        node_availmem_gb="$((node_availmem/1024/1024))"
        node_usemem_gb="$((node_totmem_gb-node_availmem_gb))"
        node_memfree_gb="${node_availmem_gb}/${node_totmem_gb}"

        printf "${node_name}\t${node_state}\t${node_memfree_gb}\t${node_coreuse}\t${node_njobs}\t${user_njobs}"
        for job in "${reserve_jobs[@]}"; do
            reserve_info=$(echo "$job" | grep "$node_name")
            if [ -n "$reserve_info" ]; then
                qstat_cols=($reserve_info)
                qstat_jobid=${qstat_cols[0]}
                qstat_user=${qstat_cols[1]}
                qstat_np=$(echo "$node_jobs" | grep -o "${qstat_jobid}" | wc -l)
                qstat_walltime=${qstat_cols[8]}
                qstat_runtime=${qstat_cols[10]}
                remain_time="$(sec2time $(( $(time2sec "$qstat_walltime") - $(time2sec "$qstat_runtime") )) )"
                printf " -- ${qstat_np} cores reserved by ${qstat_user}, ${remain_time} remaining"
            fi
        done
        printf "\n"
    done
}


print_job_table() {
    echo
    awk_cmd='BEGIN { status_arr_len=split("H,Q,R,E,C,T,W,S", status_arr, ","); for (i=1;i<=status_arr_len;i++) status_count[status_arr[i]]=0; total_count=0; } { user=$3; status=$5; '"${awk_user_filter}"' user_list[user]=0; us_count[user","status]++; status_count[status]++; total_count++; } END { nzs_arr_len=0; for (i=1;i<=status_arr_len;i++) { status=status_arr[i]; if (status_count[status]>0) nonzero_status_arr[++nzs_arr_len]=status; }; printf "%-12s", "USER"; for (i=1;i<=nzs_arr_len;i++) { status=nonzero_status_arr[i]; printf "%-8s", status; }; printf "\n"; printf "%-12s", "-----------"; for (i=1;i<=nzs_arr_len;i++) { printf "%-8s", "-------"; }; printf "\n"; for (user in user_list) { printf "%-12s", user; for (i=1;i<=nzs_arr_len;i++) { status=nonzero_status_arr[i]; count=us_count[user","status]; printf "%-8s", count; } printf "\n"; }; printf "%-12s", "-----------"; for (i=1;i<=nzs_arr_len;i++) { printf "%-8s", "-------"; }; printf "\n"; printf "%-12s", "TOTAL"; for (i=1;i<=nzs_arr_len;i++) { status=nonzero_status_arr[i]; printf "%-8s", status_count[status]; }; printf "-> %s total jobs\n", total_count; } '
    qstat -l | tail -n +3 | awk -F" " "$awk_cmd"
}


print_interactive_job_note() {
    echo
    echo "Submit an interactive job:"
    echo "    qsub -I -l nodes=<NODE_NAME>,ncpus=<nCORES>,mem=<nGB>gb,walltime=<HOURS>:00:00"
    echo "   (can give 'nodes=1' for any node, recommend nCORES >= 2, 'mem' argument optional)"
}


print_setsm_runs() {
    echo
    echo "USER         RES    SETSM REGION RUNNING                     SWIFT SITE"
    echo "-----------  -----  ---------------------------------------  ----------"
    awk_cmd='{ if ($11 == "bash" || $11 == "/bin/bash") { site="setsm"$14$15; printf "%-12s %-6s %-40s %s\n", $1, $14, $13, site; } }'
    ps aux | grep "auto_swift.sh" | awk -F" " "$awk_cmd" | sort -u
}
print_s2s_runs() {
    echo
    echo "USER         RES    S2S REGION RUNNING                       SWIFT SITE"
    echo "-----------  -----  ---------------------------------------  ----------"
    awk_cmd='{ if ($11 == "bash" || $11 == "/bin/bash") { site="s2s"$15; printf "%-12s %-6s %-40s %s\n", $1, $14, $13, site; } }'
    ps aux | grep "auto_s2s.sh" | awk -F" " "$awk_cmd" | sort -u
}


## MAIN ##

# "[<none>|runs|table|nodes|jobs|jobs-by-batch|jobs-by-node|jobs-by-user|blocks|all]"
for arg in "${info_mode_args[@]}"; do
    if [ "$arg" == "jobs" ] || [ "$arg" == "jobs-by-batch" ] || [ "$arg" == "jobs-by-node" ] || [ "$arg" == "jobs-by-user" ] || [ "$arg" == "<none>" ]; then
        print_job_info "$arg" "$blocks"
        if [ "$all" == "true" ]; then
            print_job_total
        fi
    fi
    if [ "$arg" == "nodes" ] || [ "$arg" == "<none>" ]; then
    # if [ "$arg" == "nodes" ]; then
        print_node_info
    fi
    # if [ "$arg" == "runs" ] || [ "$arg" == "<none>" ]; then
    #     print_setsm_runs
    #     print_s2s_runs
    # fi
    if [ "$arg" == "table" ] || [ "$arg" == "<none>" ]; then
        print_job_table
    fi
    echo
done
