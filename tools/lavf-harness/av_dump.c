// av_dump <file> <readahead_s> <seeks "-"|"a,b,c"> <after_s>
// Chrome-like: custom AVIO with a 32 KiB buffer; reads to readahead, then
// av_seek_frame(video, T, BACKWARD) for each seek; prints every packet
// "S <stream> <pts> <K|.>" (straight read: all; after a seek: until every
// stream is after_s past its first packet), "L <T> <first video pts>".
#include <libavformat/avformat.h>
#include <stdio.h>
#include <string.h>
static FILE *f; static int64_t pos, fsize;
static int rd(void *o, uint8_t *b, int n) { size_t r = fread(b, 1, n, f); pos += r; return r ? (int)r : AVERROR_EOF; }
static int64_t sk(void *o, int64_t off, int wh) {
    if (wh == AVSEEK_SIZE) return fsize; wh &= ~AVSEEK_FORCE;
    int64_t t = wh == SEEK_SET ? off : wh == SEEK_CUR ? pos + off : fsize + off;
    fseeko(f, t, SEEK_SET); pos = t; return t; }
int main(int argc, char **argv) {
    f = fopen(argv[1], "rb"); fseeko(f, 0, SEEK_END); fsize = ftello(f); fseeko(f, 0, SEEK_SET);
    double ra = atof(argv[2]), after = atof(argv[4]);
    av_log_set_level(AV_LOG_QUIET);
    AVFormatContext *fc = avformat_alloc_context();
    fc->pb = avio_alloc_context(av_malloc(32768), 32768, 0, NULL, rd, NULL, sk);
    fc->flags |= AVFMT_FLAG_CUSTOM_IO;
    if (avformat_open_input(&fc, NULL, NULL, NULL) < 0) return 1;
    avformat_find_stream_info(fc, NULL);
    int vi = av_find_best_stream(fc, AVMEDIA_TYPE_VIDEO, -1, -1, NULL, 0);
    AVPacket *p = av_packet_alloc();
    int straight = !strcmp(argv[3], "-");
    while (av_read_frame(fc, p) >= 0) {
        AVStream *st = fc->streams[p->stream_index];
        double t = p->pts * av_q2d(st->time_base);
        if (straight) if (!(p->flags & AV_PKT_FLAG_DISCARD)) printf("S %d %.6f %c %lld\n", p->stream_index, t, p->flags & AV_PKT_FLAG_KEY ? 'K' : '.', (long long)p->dts);
        av_packet_unref(p);
        if (!straight && t > ra) break;
    }
    if (straight) return 0;
    char list[4096]; strncpy(list, argv[3], sizeof list - 1);
    for (char *tok = strtok(list, ","); tok; tok = strtok(NULL, ",")) {
        double T = atof(tok);
        AVStream *vs = fc->streams[vi];
        av_seek_frame(fc, vi, av_rescale_q((int64_t)(T * AV_TIME_BASE), AV_TIME_BASE_Q, vs->time_base), AVSEEK_FLAG_BACKWARD);
        double first[8] = {-1,-1,-1,-1,-1,-1,-1,-1}; int done = 0, n = 0, landed = 0;
        while (!done && n++ < 20000 && av_read_frame(fc, p) >= 0) {
            int s = p->stream_index; double t = p->pts * av_q2d(fc->streams[s]->time_base);
            if (first[s] < 0) first[s] = t;
            if (s == vi && !landed) { printf("L %.3f %.6f\n", T, t); landed = 1; }
            if (!(p->flags & AV_PKT_FLAG_DISCARD)) printf("S %d %.6f %c %lld\n", s, t, p->flags & AV_PKT_FLAG_KEY ? 'K' : '.', (long long)p->dts);
            done = 1; for (int i = 0; i < (int)fc->nb_streams; i++) if (first[i] < 0 || t < first[i] + after) done = 0;
            if (t < first[s] + after) done = 0;
            av_packet_unref(p);
        }
    }
    return 0;
}
