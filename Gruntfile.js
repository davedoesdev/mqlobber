"use strict";

var path = require('path'),
    mod_path = path.join('.', 'node_modules'),
    bin_path = path.join(mod_path, '.bin'),
    nyc_path = path.join(bin_path, 'nyc'),
    grunt_path;

if (process.platform === 'win32')
{
    grunt_path = path.join(mod_path, 'grunt', 'bin', 'grunt');
}
else
{
    grunt_path = path.join(bin_path, 'grunt');
}

module.exports = function (grunt)
{
    grunt.initConfig(
    {
        jshint: {
            src: [ 'index.js', 'Gruntfile.js', 'lib/*.js', 'test/**/*.js' ],
            options: {
                node: true,
                esversion: 6
            }
        },

        mochaTest: {
            src: [ 'test/in-mem.js', 'test/tcp.js', 'test/example/example.js' ],
            options: {
                bail: true
            }
        },

        apidox: {
            input: [ 'lib/client.js',
                     'lib/client_events_doc.js',
                     'lib/server.js',
                     'lib/server_events_doc.js'],
            output: 'README.md',
            fullSourceDescription: true,
            extraHeadingLevels: 1
        },

        bgShell: {
            cover: {
                cmd: nyc_path + " -x Gruntfile.js -x \"" + path.join('test', '**') + "\" " + grunt_path + " test",
                fail: true,
                execOpts: {
                    maxBuffer: 0
                }
            },

            cover_report: {
                cmd: nyc_path + ' report -r lcov',
                fail: true
            },

            cover_check: {
                cmd: nyc_path + ' check-coverage --statements 100 --branches 100 --functions 100 --lines 100',
                fail: true
            },

            coveralls: {
                cmd: 'cat coverage/lcov.info | coveralls',
                fail: true
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-mocha-test');
    grunt.loadNpmTasks('grunt-bg-shell');
    grunt.loadNpmTasks('grunt-apidox');

    grunt.registerTask('lint', 'jshint');
    grunt.registerTask('test', 'mochaTest');
    grunt.registerTask('docs', ['apidox']);
    grunt.registerTask('coverage', ['bgShell:cover',
                                    'bgShell:cover_report',
                                    'bgShell:cover_check']);
    grunt.registerTask('coveralls', 'bgShell:coveralls');
    grunt.registerTask('default', ['lint', 'test']);
};

