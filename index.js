const express = require('express');
const bodyParser = require('body-parser');
require('./consumer');

const indexRouter = require('./routes/index');

const app = express();

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }));
// parse application/json
app.use(bodyParser.json());

app.use('/', indexRouter);

const port = process.env.PORT || 5001;
app.listen(port, () => {
    console.log(`Server is running at port ${port}`);
});

module.exports = app;