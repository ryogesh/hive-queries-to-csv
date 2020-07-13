# Copyright (c) 2020  Yogesh Rajashekharaiah
# All Rights Reserved

using ArgParse 
#using Sockets
using Dates
using JSON
using DelimitedFiles


function create_csv(csvfl, fmode, hdr, qdct, row)
    if length(qdct) > 0
        open(csvfl, fmode) do io
            if hdr
                writedlm(io, row, ',')
            end
            for key in keys(qdct)
                row = [key qdct[key]["Query"] qdct[key]["user"] qdct[key]["queueName"] qdct[key]["appId"] qdct[key]["Compile"]["Start"] qdct[key]["Compile"]["End"] qdct[key]["Compile"]["TimeTaken"] qdct[key]["Execute"]["Start"] qdct[key]["Execute"]["End"] qdct[key]["Execute"]["TimeTaken"] qdct[key]["sessionName"]]
                writedlm(io, row, ',')
            end
        end
    end
end

function create_json(jfl, fmode, qdct)
    if length(qdct) > 0
        jtxt = JSON.json(qdct)
        open(jfl, fmode) do wfl
            write(wfl, jtxt)
        end
    end
end

function get_queries(lfile, fdir, periodic, fformat)
    open(lfile) do hlog
        last_ts = "2000-01-01T00:00:01,1"
        sdts = "2000-01-01T00:00:01,2"
        qdct = Dict()
        pres_fl = "$(fdir)/.queries.dat"  # partial results file
        if periodic == "y" && isfile(pres_fl)
            jtxt = open(pres_fl, "r") do rfl
                read(rfl, String)
            end            
            if length(jtxt) > 0
                datdct = JSON.parse(jtxt)
                last_ts = datdct["ts"]
                qdct = datdct["queries"]
            end
        end
        spos = 0
        epos = 0
        tlns = 0
        plns = 0
        qid = ""
        query = ""
        mln_qry = false
        mln_dag = false
        app_id = ""
        session_name = ""
        call_rs = [' ', '}']
        ses_rs = [',', ' ']
        splt_on = ("queryId=", "): ", ");", "callerId=", "applicationId=", ": ", "sessionName=", "sessionId=", ", ", "=", ",")
        for ln in eachline(hlog)
            tlns += 1
            if occursin(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}),(\d{3})", ln)
                sdts = ln[1:23]
                #dts = DateTime(sdts, tformat)
            else
                # No DateTimeStamp on line, check for multi-line query or DAG
                if mln_qry
                    plns += 1
                    query *= ln
                elseif mln_dag
                    plns += 1
                    if occursin(splt_on[4], ln) #"callerId="
                        qid = rstrip(split(ln, splt_on[4])[2], call_rs)
                    else
                        continue
                    end
                    mln_dag = false
                    qdct[qid]["sessionName"] = session_name
                    qdct[qid]["applicationId"] = app_id
                end
                continue
            end
            # If file_line_ts < last_run ignore
            if sdts >= last_ts
                if mln_qry
                    mln_qry = false
                    qdct[qid]["Query"] = query
                end
            else
                continue
            end
            plns += 1
            if occursin("Compiling command", ln)
                spos = findfirst(splt_on[1], ln)[end] + 1        #"queryId="
                epos = findnext(splt_on[2], ln, spos)[1] - 1     # "): "
                qid = ln[spos:epos]
                query = ln[epos+4:end]
                qdct[qid] = Dict("Query"=>"",
                                 "Compile"=>Dict("Start"=>sdts,
                                                 "End"=>"",
                                                 "TimeTaken"=>""
                                                 ),
                                 "Execute"=>Dict("Start"=>"",
                                                 "End"=>"",
                                                 "TimeTaken"=>""
                                                 ),
                                 "sessionName"=>"",
                                 "sessionId"=>"",
                                 "appId"=>"",
                                 "user"=>"",
                                 "queueName"=>"",
                                )
                mln_qry = true
            elseif occursin("Completed compiling command", ln)
                spos = findfirst(splt_on[1], ln)[end] + 1        #"queryId="
                epos = findnext(splt_on[3], ln, spos)[1] - 1     # ");"
                qid = ln[spos:epos]
                spos = epos + 4
                epos = findnext(splt_on[6], ln, spos)[end]       #": "
                ttaken = ln[epos+1:end]
                if get(qdct, qid, "-a") != "-a" 
                    qdct[qid]["Compile"]["TimeTaken"] = ttaken
                    qdct[qid]["Compile"]["End"] = sdts
                end
            elseif occursin("Executing command", ln) && !occursin("interrupted", ln)
                spos = findfirst(splt_on[1], ln)[end] + 1        #"queryId="
                epos = findnext(splt_on[2], ln, spos)[1] - 1     # "): "
                qid = ln[spos:epos]
                if get(qdct, qid, "-a") != "-a"
                    qdct[qid]["Execute"]["Start"] = sdts
                end
            elseif occursin("Completed executing command", ln)
                spos = findfirst(splt_on[1], ln)[end] + 1        #"queryId="
                epos = findnext(splt_on[3], ln, spos)[1] - 1     # ");"
                qid = ln[spos:epos]
                spos = epos + 4
                epos = findnext(splt_on[6], ln, spos)[end]       #": "
                ttaken = ln[epos+1:end]
                if get(qdct, qid, "-a") != "-a"
                    qdct[qid]["Execute"]["TimeTaken"] = ttaken
                    qdct[qid]["Execute"]["End"] = sdts
                end
            elseif occursin("Submitting dag to TezSession, sessionName", ln)
                # callerId is the same as queryId
                vals = split(ln, splt_on[4])                                #"callerId="
                app = split(vals[1], splt_on[5])                            #"applicationId="
                app_id = split(app[2], splt_on[11])[1]                      #","
                session_name = rstrip(split(app[1], splt_on[7])[2], ses_rs) #"sessionName="
                if length(vals) == 2
                    qid = rstrip(vals[2], call_rs)
                else
                    # sometimes dagName is multi-line
                    mln_dag = true
                    continue
                end
                if get(qdct, qid, "-a") != "-a"
                    qdct[qid]["appId"] = app_id
                    qdct[qid]["sessionName"] = session_name
                end
            elseif occursin("Closing tez session if not default: sessionId=", ln) 
                spos = findfirst(splt_on[8], ln)[end] + 1        #"sessionId="
                epos = findnext(splt_on[9], ln, spos)[1] - 1     # ", "
                sid = ln[spos:epos]               
                spos = findnext(splt_on[10], ln, epos)[end] + 1   #"="
                epos = findnext(splt_on[9], ln, spos)[1] - 1
                qname = ln[spos:epos]
                spos = findnext(splt_on[10], ln, epos)[end] + 1
                epos = findnext(splt_on[9], ln, spos)[1] - 1
                quser = ln[spos:epos]                
                qid = ""
                for key in keys(qdct)
                    if endswith(qdct[key]["sessionName"], sid)
                        qid = key
                        break
                    end
                end
                if get(qdct, qid, "-a") != "-a"
                    qdct[qid]["user"] = quser
                    qdct[qid]["queueName"] = qname
                end
            end
        end
        #Store partial results in separate dictionary, pdct
        pdct = Dict()
        for key in keys(qdct)
            if qdct[key]["Compile"]["Start"]!="" && qdct[key]["Compile"]["End"]=="" ||
               qdct[key]["Execute"]["Start"]!="" && qdct[key]["Execute"]["End"]==""
                pdct[key] = qdct[key]
                delete!(qdct, key)
            end
        end
        #If running as periodic job, store the last_run TS and partial results
        if periodic == "y"
            datdct = Dict("queries"=>qdct, "ts"=>sdts)
            create_json(pres_fl, "w", datdct)
        end
        row = ["queryId" "Query" "user" "queueName" "applicationId" "CompileStartTime" "CompileEndTime" "CompileTime" "ExecuteStartTime" "ExecuteEndTime" "ExecuteTime" "sessionName"]
        if fformat in ("c", "b")
            fmode = "w"
            hdr = true
            #csvfl = "$(fdir)/$(getnameinfo(getipaddr()))_queries_$(today()).csv"
            csvfl = "$(fdir)/hive_queries_$(today()).csv"
            if isfile(csvfl)
                fmode = "a"
                hdr = false           
            end 
            create_csv(csvfl, fmode, hdr, qdct, row)
            #csvfl = "$(fdir)/$(getnameinfo(getipaddr()))_incomplete_queries_$(today()).csv"
            csvfl = "$(fdir)/hive_incomplete_queries_$(today()).csv"
            create_csv(csvfl, "w", true, pdct, row)
        end
        if fformat in ("j", "b")
            fmode = "w"
            #jfl = "$(fdir)/$(getnameinfo(getipaddr()))_queries_$(today()).json"
            jfl = "$(fdir)/hive_queries_$(today()).json"
            if isfile(jfl)
                fmode = "a"          
            end 
            create_json(jfl, fmode, qdct)
            #jfl = "$(fdir)/$(getnameinfo(getipaddr()))_incomplete_queries_$(today()).json"
            jfl = "$(fdir)/hive_incomplete_queries_$(today()).json"
            create_json(jfl, "w", pdct)
        end
        println("Log file total lines: $(tlns), Processed:$(plns), Total queries:$(length(qdct))")
    end
end
   
function main()
    parser = ArgParseSettings(description="Save user queries execution metrics to a csv file",
                              epilog="Make sure the program has read access to the hiveserver2 log file")
    fname = "/var/log/hive/hiveserver2.log"
    fldir = ENV["HOME"]=="" ? "/tmp" : ENV["HOME"] 
    @add_arg_table! parser begin
        "--logfile"
           help="HiveServer2 Log file location, Default:$(fname)" 
           default="$(fname)"
        "--dir"
           default="$(fldir)"
           help="Folder to save the csv files, Default:$(fldir)"
        "--periodic"
           default="n"
           help="Capture queries periodically (y), use with scheduler, Default:n"
        "--format"
           default="c"
           help="File format(csv:c, json:j, both:b, Default:csv(c)"
    end
    args = parse_args(parser)
    println("Starting query metrics collection: $(now())")
    get_queries(args["logfile"], args["dir"], args["periodic"], args["format"])
    println("Collected query metrics to csv files: $(now())")   
end

main()
