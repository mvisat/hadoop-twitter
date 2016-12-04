/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author visat
 */
public class UserWritable implements Writable, Comparable<UserWritable> {

    private DoubleWritable pageRank;
    private Text followings;


    public UserWritable() {
        pageRank = new DoubleWritable(1);
        followings = new Text();
    }

    public UserWritable(Double pageRank) {
        this.pageRank = new DoubleWritable(pageRank);
        this.followings = new Text();
    }

    public UserWritable(String following) {
        pageRank = new DoubleWritable(1);
        followings = new Text(following);
    }

    public UserWritable(Double pageRank, String followings) {
        this.pageRank = new DoubleWritable(pageRank);
        this.followings = new Text(followings);
    }

    public UserWritable(DoubleWritable pageRank, Text followings) {
        this.pageRank = pageRank;
        this.followings = followings;
    }

    public Double getPageRank() {
        return pageRank.get();
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = new DoubleWritable(pageRank);
    }

    public Text getFollowings() {
        return followings;
    }

    public void setFollowings(Text followings) {
        this.followings = followings;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        pageRank.write(out);
        followings.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageRank.readFields(in);
        followings.readFields(in);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(pageRank)
                .append(' ')
                .append(followings)
                .toString();
    }

    @Override
    public int compareTo(UserWritable o) {
        if (pageRank.get() > o.pageRank.get()) return 1;
        else if (pageRank.get() < o.pageRank.get()) return -1;
        else return followings.toString().compareTo(o.followings.toString());
    }
}
