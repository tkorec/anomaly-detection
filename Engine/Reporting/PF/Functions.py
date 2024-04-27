def domain_dataset_results(nok_data):
    results = ""
    for i in range(0, nok_data.count()):
        results += f"""
            <table class="values" style="width: 100%; margin-top: 5px; background-color: #FFE3E3; border: 1.5px solid #FBC0C0;">
                <tr>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0 10px 10px;">{nok_data.collect()[i]["domain"]} - {nok_data.collect()[i]["dataset"]}</td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0; text-align: center;">
                        value: {nok_data.collect()[i]["value"]};   
                        expected: {nok_data.collect()[i]["expected"]};  
                        difference: {nok_data.collect()[i]["difference"]}; 
                        relative difference: {nok_data.collect()[i]["relativedifference"]};
                        z-score: 2.67: {nok_data.collect()[i]["z_score"]}
                    </td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 10px 10px 0; text-align: end;">{nok_data.collect()[i]["verdict"]}</td>
                </tr>
            </table>"""
    legend = """
        <table class="legend" style="width: 100%; margin-top: 5px;">
            <tr>
                <td style="font-size: 12px; padding: 0 0 7px 0;">Domain - dataset</td>
                <td style="font-size: 12px; padding: 0 0 7px 0; text-align: end;">Problem Type [NOK, MISSING, ERROR]</td>
            </tr>
        </table>"""
    return results, legend


def domain_patternid_results(nok_data):
    results = ""
    for i in range(0, nok_data.count()):
        results += f"""
            <table class="values" style="width: 100%; margin-top: 5px; background-color: #FFE3E3; border: 1.5px solid #FBC0C0;">
                <tr>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0 10px 10px;">{nok_data.collect()[i]["domain"]} - {nok_data.collect()[i]["patternId"]}</td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0; text-align: center;">
                        value: {nok_data.collect()[i]["value"]};   
                        expected: {nok_data.collect()[i]["expected"]};  
                        difference: {nok_data.collect()[i]["difference"]}; 
                        relative difference: {nok_data.collect()[i]["relativedifference"]};
                        z-score: 2.67: {nok_data.collect()[i]["z_score"]}
                    </td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 10px 10px 0; text-align: end;">{nok_data.collect()[i]["verdict"]}</td>
                </tr>
            </table>"""
    legend = """
        <table class="legend" style="width: 100%; margin-top: 5px;">
            <tr>
                <td style="font-size: 12px; padding: 0 0 7px 0;">Domain - Pattern ID</td>
                <td style="font-size: 12px; padding: 0 0 7px 0; text-align: end;">Problem Type [NOK, MISSING, ERROR]</td>
            </tr>
        </table>"""
    return results, legend


def domain_dataset_behavior_results(nok_data):
    results = ""
    for i in range(0, nok_data.count()):
        results += f"""
            <table class="values" style="width: 100%; margin-top: 5px; background-color: #FFE3E3; border: 1.5px solid #FBC0C0;">
                <tr>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0 10px 10px;">{nok_data.collect()[i]["domain"]} - {nok_data.collect()[i]["dataset"]} – {nok_data.collect()[i]["behavior"]}</td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0; text-align: center;">
                        value: {nok_data.collect()[i]["value"]};   
                        expected: {nok_data.collect()[i]["expected"]};  
                        difference: {nok_data.collect()[i]["difference"]}; 
                        relative difference: {nok_data.collect()[i]["relativedifference"]};
                        z-score: 2.67: {nok_data.collect()[i]["z_score"]}
                    </td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 10px 10px 0; text-align: end;">{nok_data.collect()[i]["verdict"]}</td>
                </tr>
            </table>"""
    legend = """
        <table class="legend" style="width: 100%; margin-top: 5px;">
            <tr>
                <td style="font-size: 12px; padding: 0 0 7px 0;">Domain - dataset - behavior</td>
                <td style="font-size: 12px; padding: 0 0 7px 0; text-align: end;">Problem Type [NOK, MISSING, ERROR]</td>
            </tr>
        </table>"""
    return results, legend


def metric_wrapper_func(metric_name):
    metric_wrapper = f"""
        <div class="metric-wrapper" style="margin-top: 50px; width: 95%">
            <p class="metric-name" style="font-weight: 600; font-size: 14px;">{metric_name}</p>
        </div>"""
    return metric_wrapper