import model, json, requests

base_url    = 'https://chatbot.citizenlab.web.id/panel/pengaduan?based=is_replied'
target_url  = "https://onesignal.com/api/v1/notifications"
header      = {'Content-Type': 'application/json; charset=utf-8',
               'Authorization': 'Basic ZGFjM2JmYzMtNDIyNy00Y2I2LTgxMWUtZjU0NTM5NDU0MGU3'}
queryRunner = model.QueryRunner()
msgOutMod = model.MessageOut()
all = 0
problems = queryRunner.runGetResults('select count(problem_report.id) AS `amount`, problem_report.sector AS `sector` from problem_report where is_replied = 0 GROUP BY problem_report.sector')

for data in problems:

    all += data['amount']
    if(data['sector']=='infrastruktur'):
        category = 'infra'
    elif(data['sector']=='kesehatan'):
        category = 'kes'
    elif (data['sector'] == 'pendidikan'):
        category = 'pend'
    elif (data['sector'] == 'administrasi'):
        category = 'adm'
    else:
        category = 'lainnya'

    opts = [{'field' : 'tag','key' : 'category','relation' : '=','value' : category}]
    content = {'en' : '%i pengaduan %s belum terespon' % (data['amount'], data['sector'])}

    fields = {
        'app_id' : "380695d9-fee9-4e5b-b67f-f0606625600c",
        'filters' : opts,
        'contents' : content,
        'url' : base_url
    }
    fields_json = json.dumps(fields)
    requests.post(target_url,data=fields_json, headers=header)

opts = [{'field' : 'tag','key' : 'category','relation' : '=','value' : 'all'}]
content = {'en':'%i pengaduan belum terespon' % (all)}
fields = {
    'app_id' : "380695d9-fee9-4e5b-b67f-f0606625600c",
    'filters' : opts,
    'contents' : content,
    'url' : base_url
}
fields_json = json.dumps(fields)
requests.post(target_url,data=fields_json, headers=header)


