"use strict";

module.exports = function (grunt)
{
    grunt.initConfig(
    {
        jshint: {
            src: [ 'Gruntfile.js', 'lib/*.js' ],
            options: {
                node: true,
                esversion: 6
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-jshint');

    grunt.registerTask('lint', 'jshint');
    grunt.registerTask('default', ['lint']);
};

