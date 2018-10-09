'use strict';

var https = require("https");
//used to get around CANVAS API rate-limit concurrency issue
https.globalAgent.maxSockets = 20;
var AWS = require('aws-sdk');
var ADL = require('adl-xapiwrapper');

var conf = {
    "url" : "https://utscic.lrs.io/xapi/",
    "auth" : {
        user : "onlycic",
        pass : "utscic001"
    }
};

var LRS = new ADL.XAPIWrapper(conf);

// Set the region
AWS.config.update({region: 'ap-southeast-2'});

var presets = ['enrollments','discussion_topics','quizzes'];
var fetch_params = typeof process.env.CANVAS_FETCH !== 'undefined'? process.env.CANVAS_FETCH.split(','): presets;


exports.handler = async (event, context) => {

    let courses = await getCourses();
    let courseData = await getCourseData(courses);

    let users = await getUsers(courseData);
    //get just courses that have associated grps ->discussions
    let groupData = await getGroupData(courseData);
    //now run through this list and get details based on grp/id/dic_topic/id
    let groupDiscussion = await getGroupDiscussions(groupData);

    let subData = await getSubData(courseData);

    //now run to get all sub discussion data and should match the format like groupDiscussion
    let subDiscussions = await getDiscussionSubData(subData);

    //get all new users for these groups and consolidate the list with existing users
    let allUsers = await getGdUsers(groupDiscussion, users);

    //get all new users for these sub discussions and consolidate the list with existing users
    let final_users = await getSdUsers(subDiscussions, allUsers);

    let statements = await generateStatements(courseData, final_users);

    let gStatements = await generateGdStatements(groupDiscussion, final_users);

    let sStatements = await generateSdStatements(subDiscussions, final_users);

    let allStmts = statements.concat(gStatements);

    let final_stmts = allStmts.concat(sStatements);
    let inserted = await insertIntoLRS(final_stmts);

    return inserted;

};

async function getCourses() {
    let body = '';
    return new Promise((resolve, reject) => {
        const options = {
            host: process.env.CANVAS_API_HOST,
            path: '/api/v1/courses',
            method: 'GET',
            headers: {
                'Cache-Control': 'no-cache',
                'Authorization': 'Bearer '+process.env.CANVAS_ACCESS_TOKEN,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        };

        const req = https.request(options, (res) => {
            res.on('data', (chunk) => {
                body += chunk;
            });

            res.on('end', () => {
               // let tmp = JSON.stringify(body);
                var result = JSON.parse(body);
                return resolve(result);
            });
        });

        req.on('error', (e) => {
            reject(e.message);
        });
        // send the request
        req.write('');
        req.end();
    });
}


async function getCourseData(courses) {

       let pCourses = courses.map( async (course) => {
           let jsonVal = {};
           fetch_params.forEach(function(proc) {
               jsonVal[proc] ='';
           });

           let param = {};
           let result = fetch_params.map(async (proc) => {
               return {
                   ...param,
                   [proc] : await fetchData(course.id, proc, jsonVal)
               }
           });

           const ts = await Promise.all(result);
           ts.push({'course':course});
           let resultObject = ts.reduce(function(result, currentObject) {
               for(let key in currentObject) {
                   if (currentObject.hasOwnProperty(key)) {
                           result[key] = currentObject[key];
                   }
               }
               return result;
           }, {});
           return resultObject;
       });
    const data_extract = await Promise.all(pCourses);
    return data_extract;
}


async function generateStatements(courseData, users) {

    let list = courseData.map( (details) => {
        let stmt =  makeStatements(details.course.id, details.discussion_topics, users);
        return stmt;
    });
    let lists = [];
    list.forEach(ls => lists.push(...ls));
    return lists;
}

async function generateGdStatements(gdData, users) {

    let list = gdData.map( (details) => {
        let stmt =  makeGdStatements(details, users);
        return stmt;
    });
    let lists = [];
    list.forEach(ls => lists.push(...ls));
    return lists;
}

async function generateSdStatements(sdData, users) {

    let list = sdData.map( (details) => {
        let stmt =  makeSdStatements(details, users);
        return stmt;
    });
    let lists = [];
    list.forEach(ls => lists.push(...ls));
    return lists;
}


async function getUsers(courseData) {

    let userList = courseData.map( (details) => {
        let du = details.discussion_topics.reduce( (usr, entry) => {
            usr.push(entry.author.id);
            return usr;
        }, []);
         let filtered = du.filter( (n, pos) => {
             return du.indexOf(n) === pos;
        });
        return filtered;
    });
    let lists = [];
    userList.forEach(ls => lists.push(...ls));

    let usrDetails = lists.map(async (idx) => {
        let ds = await fetchUser(idx);
        return ds;
    });

    const details = await Promise.all(usrDetails);
    return details;
}

async function getGdUsers(groupDiscussion, users) {
    let details = [];
    let lists = [];
    //just get the list of new participants that do not exists in course--discussion_topics

    if(groupDiscussion.length > 0 ) {

       groupDiscussion.forEach( (t) => {
            if(t.discussions.length > 0) {
                t.discussions.forEach( (gd) =>{
                    if(gd.discussions.participants.length >0 && users.length >0) {
                        gd.discussions.participants.forEach ((new_usr) => {
                            let user_found = users.find((usr) => usr.id === new_usr.id);
                            if(user_found ===undefined) {
                                lists.push(new_usr.id);
                            }
                        });
                    }
                });
            }
        });

        //if new users get their details
        if(lists.length > 0 ) {
            let usrDetails = lists.map(async (idx) => {
                let ds = await fetchUser(idx);
                return ds;
            });
            let more_users = details = await Promise.all(usrDetails);
            if(more_users.length > 0 ) {
                details = users.concat(more_users);
            }
        } else {
            details = users;
        }
    }
    return details;
}

async function getSdUsers(subDiscussion, users) {
    let details = [];
    let lists = [];
    //just get the list of new participants that do not exists in course--discussion_topics

    if(subDiscussion.length > 0 ) {

        subDiscussion.forEach( (t) => {
            t.forEach( (sd) => {

                if (sd.discussions.participants.length > 0 && users.length > 0) {
                    sd.discussions.participants.forEach((new_usr) => {

                        let user_found = users.find((usr) => usr.id === new_usr.id);
                        if (user_found===undefined) {
                            lists.push(new_usr.id);
                        }
                    });
                }
            });
        });

        //if new users get their details
        let more_users =[];
        if(lists.length > 0 ) {
            let usrDetails = lists.map(async (idx) => {
                let ds = await fetchUser(idx);
                return ds;
            });
            let more_users = details = await Promise.all(usrDetails);
            if(more_users.length > 0 ) {
                details = users.concat(more_users);
            }
        } else {
            details = users;
        }
    }
    return details;
}


async function insertIntoLRS(statements) {

    let result = statements.map( async (stmt) => {
        let ts = await insertLRS(stmt);
        return ts;
    });
    const data_extract = await Promise.all(result);
    return data_extract;
}

function insertLRS(stmts) {
    return new Promise((resolve, reject) => {
        LRS.sendStatements(stmts, (err, res) => {
            if(err) return reject(err);
            return resolve('Added');
        });
    });
}

async function getGroupData(courseData) {
    let groups = [];
    courseData.forEach( (details) => {
        let c = {};
        c.course_id = details.course.id;
        c.course_name = details.course.name;
        let du = details.discussion_topics.filter( (entry) => {
            if(entry.group_topic_children.length > 0) {
                return entry.group_topic_children;
            }
        });

        if(du.length > 0 ) {
            groups.push({
                ['course']: c,
                ['data']: du[0]
            });
        }

    });
    return groups;
}

async function getSubData(courseData) {

    let subs = courseData.filter( (details) => {
        if(details.discussion_topics.length > 0 ) {
            return details;
        }

    });
    return subs;
}


async function getDiscussionSubData(subs) {

    let overall = subs.map( async(x) => {
        let c = {};
        c.course_id = x.course.id;
        c.course_name = x.course.name;
        let cnt =0;
        let sd = x.discussion_topics.map(async (subDetails) => {
            let i = await fetchSubDiscussion(subDetails.id, x.course.id);
            let pd = {};
            pd.discussion_id=subDetails.id;
            pd.discussion_title= subDetails.title;
            return {course: c, parent_discussion: pd, discussions: i}
        });
        return await Promise.all(sd);
    });

    return await Promise.all(overall);



    //return subs;
}


async function getGroupDiscussions(data) {
    let grp ={};
    let groupdata = data.map( async (g) => {

        let gd = g.data.group_topic_children.map( async(details) => {
            let h = await fetchGroupDiscussion(details.id, details.group_id);
            return { group_id: details.group_id, discussion_id: details.id, discussions: h}
        });

        const group_data = await Promise.all(gd);
        return {
            ...grp,
            ['course']: g.course,
            ['parent_discussion']: {discussion_id: g.data.id, discussion_title: g.data.title},
            ['discussions']: group_data
        }
    });

    return await Promise.all(groupdata);
}

function fetchData(id, action, jsonVal) {

    return new Promise((resolve, reject) => {
        const options1 = {
            host: process.env.CANVAS_API_HOST,
            path: '/api/v1/courses/'+id+'/'+action+'?per_page=50',
            method: 'GET',
            headers: {
                'Cache-Control': 'no-cache',
                'Authorization': 'Bearer '+process.env.CANVAS_ACCESS_TOKEN,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        };
        const req1 = https.request(options1, (res) => {
            res.on('data', (chunk) => {
                jsonVal[action] += chunk;
            });

            res.on('end', () => {
                return resolve(JSON.parse(jsonVal[action]));
            });
        });

        req1.on('error', (e) => {
            reject(e.message);
        });
        // send the request
        req1.write('');
        req1.end();
        //return resolve(enrollments);
    });
}

function fetchUser(id) {
    let bdy ='';
    return new Promise((resolve, reject) => {
        const options1 = {
            host: process.env.CANVAS_API_HOST,
            path: '/api/v1/users/'+id+'/profile',
            method: 'GET',
            headers: {
                'Cache-Control': 'no-cache',
                'Authorization': 'Bearer '+process.env.CANVAS_ACCESS_TOKEN,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        };
        const req1 = https.request(options1, (res) => {
            res.on('data', (chunk) => {
                bdy += chunk;
            });

            res.on('end', () => {
                let dt =JSON.parse(bdy);
                return resolve(dt);
            });
        });

        req1.on('error', (e) => {
            reject(e.message);
        });
        // send the request
        req1.write('');
        req1.end();
        //return resolve(enrollments);
    });
}

function makeStatements (id, discussions, users) {
    let statements = [];
    discussions.forEach( (discussion) => {
        let stmt = cloneStatement();
        stmt.actor.account.homePage = "https://canvas.uts.edu.au/profile";
        if(discussion.author.id === undefined) {
            stmt.actor.name = 'Kirsty Kitto';
            stmt.actor.account.name = "kirsty.kitto@uts.edu.au";
        } else {
            let user = users.find((usr) => usr.id === discussion.author.id);
            if(typeof user == 'undefined') {
                stmt.actor.name = 'Kirsty Kitto';
                stmt.actor.account.name = "kirsty.kitto@uts.edu.au";
            }
            else {
                stmt.actor.name = user.name === undefined ? 'Kirsty Kitto' : user.name;
                stmt.actor.account.name = (user.login_id === undefined ? "kirsty.kitto@uts.edu.au" : user.login_id.toLowerCase());
            }
        }
        //stmt.actor.account.homePage = discussion.author.html_url;
        stmt.actor.objectType = "Agent";
        //verbs
        stmt.verb.id = "http://activitystrea.ms/create";
        stmt.verb.display["en-US"] = "created";
        //object
        stmt.object.id=discussion.html_url;
        stmt.object.objectType ="Activity";
        stmt.object.definition.name["en-US"] = "Note";
        stmt.object.definition.description["en-US"] = discussion.message;
        //context
       // stmt.context.timestamp = discussion.posted_at;
        stmt.context.platform = "Canvas";
        stmt.context.contextActivities.category.push ({"id" : "http://activitystrea.ms/schema/1.0.0"});
        stmt.context.contextActivities.parent.push ({"id" : "https://"+process.env.CANVAS_API_HOST+"/courses/"+id});
        stmt.context.contextActivities.grouping.push ({"id" :discussion.html_url});
        stmt.timestamp = new Date(discussion.posted_at);
        statements.push(stmt);
    });
    return statements;
}

function makeGdStatements (discussions, users) {

    let gdStatements = [];

        let course = discussions.course;
        let parent = discussions.parent_discussion;
        discussions.discussions.forEach((ind) => {
            let group_id = ind.group_id;
            let discussion_id = ind.discussion_id;
            ind.discussions.view.forEach((vi) => {
                let stmt = cloneStatement();
                stmt.actor.account.homePage = "https://canvas.uts.edu.au/profile";

                    let user = users.find((usr) => usr.id === vi.user_id);
                    if (typeof user == 'undefined') {
                        stmt.actor.name = 'Kirsty Kitto';
                        stmt.actor.account.name = "kirsty.kitto@uts.edu.au";
                    }
                    else {
                        stmt.actor.name = user.name === undefined ? 'Kirsty Kitto' : user.name;
                        stmt.actor.account.name = (user.login_id === undefined ? "kirsty.kitto@uts.edu.au" : user.login_id);
                    }


                //verbs
                stmt.verb.id = "http://activitystrea.ms/create";
                stmt.verb.display["en-US"] = "created";
                //object
                stmt.object.id = "https://canvas.uts.edu.au/courses/"+ course.course_id+"/discussion_topics/"+vi.id;
                stmt.object.objectType = "Activity";
                stmt.object.definition.name["en-US"] = "Note";
                stmt.object.definition.description["en-US"] = vi.message;
                //context
                // stmt.context.timestamp = discussion.posted_at;
                stmt.context.platform = "Canvas";
                stmt.context.contextActivities.category.push({"id": "http://activitystrea.ms/schema/1.0.0"});
                stmt.context.contextActivities.parent.push({
                        "id": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id,
                        "definition" : {
                            "name": {
                                "en-US": course.course_name
                            },
                            "type": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id
                        }
                });
                stmt.context.contextActivities.grouping.push({
                    "id":"https://canvas.uts.edu.au/courses/"+ course.course_id+"/discussion_topics/"+parent.discussion_id,
                    "definition": {
                        "name": {
                            "en-US":parent.discussion_title
                        },
                        "type": "https://canvas.uts.edu.au/courses/"+ course.course_id+"/discussion_topics/"+parent.discussion_id
                    }
                });
                stmt.timestamp = new Date(vi.created_at);
                gdStatements.push(stmt);


                if(typeof vi.replies !== 'undefined') {
                    vi.replies.forEach((vvi) => {
                        let stmt = cloneStatement();
                        stmt.actor.account.homePage = "https://canvas.uts.edu.au/profile";

                        let user = users.find((usr) => usr.id === vi.user_id);
                        if (typeof user == 'undefined') {
                            stmt.actor.name = 'Kirsty Kitto';
                            stmt.actor.account.name = "kirsty.kitto@uts.edu.au";
                        }
                        else {
                            stmt.actor.name = user.name === undefined ? 'Kirsty Kitto' : user.name;
                            stmt.actor.account.name = (user.login_id === undefined ? "kirsty.kitto@uts.edu.au" : user.login_id);
                        }


                        //verbs
                        stmt.verb.id = "http://activitystrea.ms/create";
                        stmt.verb.display["en-US"] = "replied";
                        //object
                        stmt.object.id = "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vvi.id;
                        stmt.object.objectType = "Activity";
                        stmt.object.definition.name["en-US"] = "Note";
                        stmt.object.definition.description["en-US"] = vvi.message;
                        //context
                        // stmt.context.timestamp = discussion.posted_at;
                        stmt.context.platform = "Canvas";
                        stmt.context.contextActivities.category.push({"id": "http://activitystrea.ms/schema/1.0.0"});
                        stmt.context.contextActivities.parent.push({
                            "id": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id,
                            "definition": {
                                "name": {
                                    "en-US": course.course_name
                                },
                                "type": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id
                            }
                        });
                        stmt.context.contextActivities.grouping.push({
                            "id": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vi.id,
                            "definition": {
                                "name": {
                                    "en-US": parent.discussion_title
                                },
                                "type": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vi.id
                            }
                        });
                        stmt.timestamp = new Date(vi.created_at);
                        gdStatements.push(stmt);
                    });
                }

            });
        });

    return gdStatements;
}

function makeSdStatements (discussions, users) {
    let sdStatements = [];
    discussions.forEach ( (first) => {
        let course = first.course;
        let parent = first.parent_discussion;
        first.discussions.view.forEach((vi) => {
             let stmt = cloneStatement();
                stmt.actor.account.homePage = "https://canvas.uts.edu.au/profile";

                let user = users.find((usr) => usr.id === vi.user_id);

                if (user !== undefined) {

                    stmt.actor.name = user.name === undefined ? 'Kirsty Kitto' : user.name;
                    stmt.actor.account.name = (user.login_id === undefined ? "kirsty.kitto@uts.edu.au" : user.login_id.toLowerCase());


                    //verbs
                    stmt.verb.id = "http://activitystrea.ms/create";
                    stmt.verb.display["en-US"] = "created";
                    //object
                    stmt.object.id = "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vi.id;
                    stmt.object.objectType = "Activity";
                    stmt.object.definition.name["en-US"] = "Note";
                    stmt.object.definition.description["en-US"] = vi.message;
                    //context
                    // stmt.context.timestamp = discussion.posted_at;
                    stmt.context.platform = "Canvas";
                    stmt.context.contextActivities.category.push({"id": "http://activitystrea.ms/schema/1.0.0"});
                    stmt.context.contextActivities.parent.push({
                        "id": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id,
                        "definition": {
                            "name": {
                                "en-US": course.course_name
                            },
                            "type": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id
                        }
                    });
                    stmt.context.contextActivities.grouping.push({
                        "id": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + parent.discussion_id,
                        "definition": {
                            "name": {
                                "en-US": parent.discussion_title
                            },
                            "type": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + parent.discussion_id
                        }
                    });
                    stmt.timestamp = new Date(vi.created_at);
                    sdStatements.push(stmt);


                    if (typeof vi.replies !== 'undefined') {
                        vi.replies.forEach((vvi) => {
                            let stmt = cloneStatement();
                            stmt.actor.account.homePage = "https://canvas.uts.edu.au/profile";

                            let user = users.find((usr) => usr.id === vvi.user_id);
                            if (user !== undefined) {

                                stmt.actor.name = user.name === undefined ? 'Kirsty Kitto' : user.name;
                                stmt.actor.account.name = (user.login_id === undefined ? "kirsty.kitto@uts.edu.au" : user.login_id.toLowerCase());


                                //verbs
                                stmt.verb.id = "http://activitystrea.ms/replied";
                                stmt.verb.display["en-US"] = "replied";
                                //object
                                stmt.object.id = "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vvi.id;
                                stmt.object.objectType = "Activity";
                                stmt.object.definition.name["en-US"] = "Note";
                                stmt.object.definition.description["en-US"] = vvi.message;
                                //context
                                // stmt.context.timestamp = discussion.posted_at;
                                stmt.context.platform = "Canvas";
                                stmt.context.contextActivities.category.push({"id": "http://activitystrea.ms/schema/1.0.0"});
                                stmt.context.contextActivities.parent.push({
                                    "id": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id,
                                    "definition": {
                                        "name": {
                                            "en-US": course.course_name
                                        },
                                        "type": "https://" + process.env.CANVAS_API_HOST + "/courses/" + course.course_id
                                    }
                                });
                                stmt.context.contextActivities.grouping.push({
                                    "id": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vi.id,
                                    "definition": {
                                        "name": {
                                            "en-US": parent.discussion_title
                                        },
                                        "type": "https://canvas.uts.edu.au/courses/" + course.course_id + "/discussion_topics/" + vi.id
                                    }
                                });
                                stmt.timestamp = new Date(vi.created_at);
                                sdStatements.push(stmt);
                            }
                        });
                    }
                }
            });
    });
    return sdStatements;
}



function cloneStatement() {
    let statement = {
        "actor": {
            "objectType": "Agent",
            "name": "",
            "account": {
                "homePage" : "",
                "name": "",
            }

        },
        "verb": {
            "id": "",
            "display": {"en-US": ""}
        },
        "object": {
            "id": "",
            "definition": {
                "name": {"en-US": ""},
                "description": {"en-US": ""}
            },
            "objectType": "Activity"
        },
        "context": {
            "platform": "",
            <!--  replyTo should have "statement" here! -->
            "contextActivities": {
                "category" : [
                ],
                "parent" : [
                ],
                "grouping" : [
                ]
            }
        },
        "timestamp": new Date()
    };
    return Object.assign({}, statement);
}

function fetchGroupDiscussion(id, group_id) {
    let gdbd = '';
    return new Promise((resolve, reject) => {
        const options2 = {
            host: process.env.CANVAS_API_HOST,
            path: '/api/v1/groups/'+ group_id+ '/discussion_topics/'+id+'/view?per_page=50',
            method: 'GET',
            headers: {
                'Cache-Control': 'no-cache',
                'Authorization': 'Bearer '+process.env.CANVAS_ACCESS_TOKEN,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        };

        const req2 = https.request(options2, (res) => {
            res.on('data', (chunk) => {
                gdbd += chunk;
            });

            res.on('end', () => {
                return resolve(JSON.parse(gdbd));
            });
        });

        req2.on('error', (e) => {
            reject(e.message);
        });
        // send the request
        req2.write('');
        req2.end();
        //return resolve(enrollments);
    });
}


function fetchSubDiscussion(id, course_id) {

     return new Promise((resolve, reject) => {
        const options = {
            host: process.env.CANVAS_API_HOST,
            path: '/api/v1/courses/'+course_id+'/discussion_topics/'+id+'/view',
            method: 'GET',
            headers: {
                'Cache-Control': 'no-cache',
                'Authorization': 'Bearer '+process.env.CANVAS_ACCESS_TOKEN,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        };

        const request = https.request(options, (response) => {
            var rdata = "";
            response.on('data', (chunk) => {
               rdata += chunk;
            });
            response.on('end', () => {
               return resolve (JSON.parse(rdata));
            });
        });

        request.on('error', (e) => {
            console.log(e);
            reject(e.message);
        });
        // send the request
        request.write('');
        request.end();

    });

}