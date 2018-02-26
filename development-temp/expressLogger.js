const express = require('express');
var bodyParser = require('body-parser');
const app = express();
let totalNum = 0;
app.use(bodyParser.json());
app.post('/', (req, res) => {
    setTimeout(() => {
        console.log(req.body);
        totalNum++;
        console.log(req.body.msg + ' (partition: ' + req.body.partition + ')');
        res.json(req.body);
    }, 500);
});
app.get('/num', (req, res) => {
    res.status(200);
    res.json({
        totalNum: totalNum
    });
});

app.listen(5554, () => console.log('queue-testing app listening on port 5554!'));