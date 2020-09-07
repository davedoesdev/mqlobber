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
            extraHeadingLevels: 1,
            doxOptions: { skipSingleStar: true }
        },

        exec: {
            cover: {
                cmd: nyc_path + " -x Gruntfile.js -x \"" + path.join('test', '**') + "\" " + grunt_path + " test"
            },

            cover_report: {
                cmd: nyc_path + ' report -r lcov'
            },

            cover_check: {
                cmd: nyc_path + ' check-coverage --statements 100 --branches 100 --functions 100 --lines 100'
            },

            coveralls: {
                cmd: 'cat coverage/lcov.info | coveralls'
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-mocha-test');
    grunt.loadNpmTasks('grunt-exec');
    grunt.loadNpmTasks('grunt-apidox');

    grunt.registerTask('lint', 'jshint');
    grunt.registerTask('test', 'mochaTest');
    grunt.registerTask('docs', ['apidox']);
    grunt.registerTask('coverage', ['exec:cover',
                                    'exec:cover_report',
                                    'exec:cover_check']);
    grunt.registerTask('coveralls', 'exec:coveralls');
    grunt.registerTask('default', ['lint', 'test']);
};

